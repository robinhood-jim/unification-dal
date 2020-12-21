package com.robin.dal.dao.cdc;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.cache.*;
import com.robin.comm.resaccess.writer.KafkaResourceWriter;
import com.robin.comm.util.config.YamlParser;
import com.robin.core.base.dao.SimpleJdbcDao;
import com.robin.core.base.datameta.*;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.core.fileaccess.util.AvroUtils;
import com.robin.core.fileaccess.writer.IResourceWriter;
import com.robin.hadoop.hdfs.HDFSUtil;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
public class  MySqlBinLogScanner implements Callable<Integer> {
    private BaseDataBaseMeta meta;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private long logPos = 0L;
    private String binlogFileName = null;
    private static BinaryLogClient client;
    private String yamlCfgFile;
    private String processYamlFile;
    private Map<String, Object> configMap;
    private HDFSUtil util;
    private Map<String, List<Integer>> syncMap = new HashMap<String, List<Integer>>();
    private Map<String, Map<Integer, DataBaseColumnMeta>> tableCfgMap = new HashMap<String, Map<Integer, DataBaseColumnMeta>>();
    private Map<String, List<DataBaseColumnMeta>> tableColumnMap = new HashMap<String, List<DataBaseColumnMeta>>();
    private Map<Long, String> tableeventMap = new ConcurrentHashMap<Long, String>();
    private Map<String, Schema> schemaMap = new HashMap<String, Schema>();
    private DataCollectionMeta colmeta;
    private String dbNamespace = "";
    private String dataSourceName="";
    private static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(3);
    private IResourceWriter writer;


    private Map<String, Map<Integer, DataSetColumnMeta>> tabcolumnMap;

    public MySqlBinLogScanner(String yamlCfgFile, String processYamlFile) {
        this.yamlCfgFile = yamlCfgFile;
        this.processYamlFile = processYamlFile;
    }


    @Override
    public Integer call() throws Exception {
        try {
            configMap = YamlParser.parseYamlFileWithClasspath(yamlCfgFile, this.getClass());
            colmeta = parseSystemConfig();
            util = new HDFSUtil(colmeta);
            parseBinLogConfig();
            getDbMeta();
            checkBinlogMetaTable();
            writer = new KafkaResourceWriter(colmeta);
            //writer.initalize();

            final LoadingCache<Long, Integer> cache = CacheBuilder.newBuilder().initialCapacity(1000).expireAfterAccess(30, TimeUnit.MINUTES).removalListener((RemovalListener<Long, Integer>) removalNotification -> tableeventMap.remove(removalNotification.getKey())).build(new CacheLoader<Long, Integer>() {
                @Override
                public Integer load(Long longval) throws Exception {
                    return 0;
                }
            });

            getLastAccess();

            client = new BinaryLogClient(meta.getParam().getHostName(), meta.getParam().getPort(), meta.getParam().getUserName(), meta.getParam().getPasswd());
            if(logPos!=0L){
                client.setBinlogPosition(logPos);
                client.setBinlogFilename(binlogFileName);
            }
            BinaryLogClient.EventListener eventListener = event -> {
                Map<String, Object> recordMap = null;
                EventType type = event.getHeader().getEventType();
                logger.info("incoming event {}", event);

                if (type == EventType.TABLE_MAP) {
                    TableMapEventData data = event.getData();
                    cache.getUnchecked(data.getTableId());
                    if (canTabBeScanned(data.getTable()))
                        tableeventMap.put(data.getTableId(), data.getTable());
                } else if (type == EventType.EXT_WRITE_ROWS) {
                    WriteRowsEventData data = event.getData();
                    long tableId = data.getTableId();
                    if (canTabBeScannedWithType(tableeventMap.get(tableId), 1)) {
                        recordMap = parseRecord(tableId, data.getIncludedColumns(), data.getRows().get(0));
                        //writer.writeRecord(recordMap);
                    }
                } else if (type == EventType.EXT_UPDATE_ROWS) {
                    UpdateRowsEventData data = event.getData();
                    Long tableId = data.getTableId();
                    if (canTabBeScannedWithType(tableeventMap.get(tableId), 2)) {
                        List<Map.Entry<Serializable[], Serializable[]>> list = data.getRows();
                        recordMap = parseRecord(tableId, data.getIncludedColumns(), list.get(0).getValue());
                        recordMap.put("_UPDATE", true);
                        //writer.writeRecord(recordMap);
                    }
                } else if (type == EventType.EXT_DELETE_ROWS) {
                    DeleteRowsEventData data = event.getData();
                    Long tableId = data.getTableId();
                    if (canTabBeScannedWithType(tableeventMap.get(tableId), 3)) {
                        recordMap = parseRecord(tableId, data.getIncludedColumns(), data.getRows().get(0));
                        recordMap.put("_DELETE", true);
                        //writer.writeRecord(recordMap);
                    }
                } else if (type == EventType.ROTATE) {
                    EventData eventData = event.getData();
                    RotateEventData rotateEventData;
                    if (eventData instanceof EventDeserializer.EventDataWrapper) {
                        rotateEventData = (RotateEventData) ((EventDeserializer.EventDataWrapper) eventData).getInternal();
                    } else {
                        rotateEventData = (RotateEventData) eventData;
                    }
                    logPos = rotateEventData.getBinlogPosition();
                    binlogFileName = rotateEventData.getBinlogFilename();
                    presistLastAccess();
                    logger.info("binlog rotate to " + rotateEventData.getBinlogFilename() + " and pos " + rotateEventData.getBinlogPosition());
                } else if (type == EventType.QUERY) {
                    QueryEventData data = event.getData();
                    String database = data.getDatabase();
                    int errCode = data.getErrorCode();
                    String sql = data.getSql();
                    if (errCode == 0) {
                        try {
                            Map<String, Object> map = new HashMap<String, Object>();
                            if (!sql.equalsIgnoreCase("BEGIN")) {
                                Statement statement = CCJSqlParserUtil.parse(sql);
                                //logger.info("sql {}",statement);
                                if (statement instanceof Insert) {
                                    Insert insert = (Insert) statement;
                                    logger.info("insert {} {} {} ", insert.getTable(), insert.getColumns(), insert.getItemsList());
                                    ExpressionList exp = (ExpressionList) insert.getItemsList();

                                    if (insert.getColumns().size() == exp.getExpressions().size()) {
                                        for (int i = 0; i < insert.getColumns().size(); i++) {
                                            Column column = insert.getColumns().get(i);
                                            Expression expression = exp.getExpressions().get(i);
                                            FillValue(column, expression, map);
                                            map.put(column.getColumnName(), exp.getExpressions().get(i));
                                        }
                                    }
                                    writer.writeRecord(map);
                                } else if (statement instanceof Update) {
                                    Update update = (Update) statement;
                                    logger.info("update {} {} {} {}", update.getTables(), update.getColumns(), update.getExpressions(), update.getWhere());
                                    if (update.getTables().size() == 1) {
                                        for (int i = 0; i < update.getColumns().size(); i++) {
                                            Column column = update.getColumns().get(i);
                                            Expression expression = update.getExpressions().get(i);
                                            FillValue(column, expression, map);
                                        }
                                        ExpressionList list = (ExpressionList) update.getWhere();
                                        for (int j = 0; j < list.getExpressions().size(); j++) {
                                            Expression expression = list.getExpressions().get(j);
                                            //FillValue(column, expression, map);
                                        }
                                    }

                                } else if (statement instanceof Delete) {
                                    Delete delete = (Delete) statement;
                                    logger.info("delete {} {} {}", delete.getTable(), delete.getWhere());
                                }
                            }
                        } catch (Exception ex) {

                        }
                    }
                }
            };
            EventDeserializer eventDeserializer = new EventDeserializer();
            eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
                    EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG);
            client.registerEventListener(eventListener);
            client.setEventDeserializer(eventDeserializer);
            client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient binaryLogClient) {
                    logPos = client.getBinlogPosition();
                    binlogFileName = client.getBinlogFilename();
                }

                @Override
                public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {
                    logPos = client.getBinlogPosition();
                    binlogFileName = client.getBinlogFilename();
                    presistLastAccess();
                }

                @Override
                public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {
                    logPos = client.getBinlogPosition();
                    binlogFileName = client.getBinlogFilename();
                    presistLastAccess();
                }

                @Override
                public void onDisconnect(BinaryLogClient binaryLogClient) {
                    logPos = client.getBinlogPosition();
                    binlogFileName = client.getBinlogFilename();
                    presistLastAccess();
                }
            });
            client.setKeepAlive(true);
            client.connect(DEFAULT_TIMEOUT);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return 0;
    }
    private void presistLastAccess(){
        Connection connection= SimpleJdbcDao.getConnection(meta,meta.getParam());
        try{
            SimpleJdbcDao.executeUpdate(connection,"insert into $_binlog(position,binlogfilename) values (?,?)",new Object[]{logPos,binlogFileName});
        }catch (Exception ex){

        }finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            }catch (Exception ex){
                logger.error("",ex);
            }
        }
    }
    private void getLastAccess(){
        Connection connection= SimpleJdbcDao.getConnection(meta,meta.getParam());
        try{
            List<Map<String,String>> list=SimpleJdbcDao.queryString(connection,"select * from  $_binlog");
            logPos=Long.valueOf(list.get(0).get("position"));

        }catch (Exception ex){

        }finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            }catch (Exception ex){
                logger.error("",ex);
            }
        }
    }
    private void checkBinlogMetaTable(){
        if(!syncMap.containsKey("$_BINLOG")) {
            Connection connection = SimpleJdbcDao.getConnection(meta, meta.getParam());
            try {
                SimpleJdbcDao.executeUpdate(connection,"create table $_binlog(position long,binlogfilename varchar(256))");
            } catch (Exception ex) {

            }finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                }catch (Exception ex){
                    logger.error("",ex);
                }
            }
        }


    }

    private void FillValue(Column column, Expression expression, Map<String, Object> map) {

        if (expression instanceof LongValue) {
            map.put(column.getColumnName(), ((LongValue) expression).getValue());
        } else if (expression instanceof DoubleValue) {
            map.put(column.getColumnName(), ((DoubleValue) expression).getValue());
        } else if (expression instanceof DateValue) {
            map.put(column.getColumnName(), ((DateValue) expression).getValue());
        } else if (expression instanceof TimeValue) {
            map.put(column.getColumnName(), ((TimeValue) expression).getValue());
        } else if (expression instanceof TimestampValue) {
            map.put(column.getColumnName(), ((TimestampValue) expression).getValue());
        } else if (expression instanceof StringValue) {
            map.put(column.getColumnName(), ((StringValue) expression).getValue());
        }
    }

    private boolean canTabBeScanned(String tableName) {
        if (!syncMap.isEmpty()) {
            if ((syncMap.containsKey(tableName))) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    private boolean canTabBeScannedWithType(String tableName, Integer dataType) {
        if (!syncMap.isEmpty()) {
            if ((syncMap.containsKey(tableName))) {
                if (syncMap.get(tableName).contains(dataType)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    private Map<String, Object> parseRecord(long tableId, BitSet includeColumns, Serializable[] rows) {
        List<Integer> columnposList = getAvaiableColumn(includeColumns);
        Map<String, Object> map = new HashMap<String, Object>();
        String tableName = tableeventMap.get(tableId);
        return retreiveRecord(tableName, rows, columnposList);
    }

    public DataCollectionMeta parseSystemConfig() {
        DataCollectionMeta meta = new DataCollectionMeta();
        Iterator<String> keyIter = configMap.keySet().iterator();
        while (keyIter.hasNext()) {
            String key = keyIter.next();
            if (key.contains("hdfs")) {
                Map<String, Object> tmap = (Map<String, Object>) configMap.get("hdfs");
                String defaultName = tmap.get("defaultName").toString();
                meta.getResourceCfgMap().put(Const.HDFS_NAME_HADOOP2, defaultName);
                if (tmap.containsKey("haconfig")) {
                    Map<String, String> haMap = (Map<String, String>) tmap.get("haconfig");
                    meta.getResourceCfgMap().putAll(haMap);
                }
            } else if (key.equals("kafka")) {
                Map<String, Object> map = (Map<String, Object>) configMap.get("kafka");
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    meta.getResourceCfgMap().put("kafka." + entry.getKey(), entry.getValue());
                }
            }
        }

        return meta;
    }

    public void parseBinLogConfig() throws Exception {
        //Map<String,Object> cmap=YamlParser.parseYamlFileWithStream(util.getHDFSDataByInputStream(processYamlFile));
        Map<String, Object> cmap = YamlParser.parseYamlFileWithClasspath(processYamlFile, this.getClass());
        Map<String, Object> dbConfigMap = null;
        if (cmap.containsKey("binlog")) {
            Map<String, Object> tmap = (Map<String, Object>) cmap.get("binlog");
            if (tmap.containsKey("dbconfig")) {
                dbConfigMap = (Map<String, Object>) tmap.get("dbconfig");
                int port = 0;
                if(dbConfigMap.containsKey("sourceName")){
                    dataSourceName=dbConfigMap.get("sourceName").toString();
                }
                if (dbConfigMap.containsKey("port")) {
                    port = Integer.valueOf(dbConfigMap.get("port").toString());
                }
                if (dbConfigMap.containsKey("namespace"))
                    dbNamespace = dbConfigMap.get("namespace").toString();
                DataBaseParam param = new DataBaseParam(dbConfigMap.get("hostName").toString(), port, dbConfigMap.get("dbName").toString(), dbConfigMap.get("user").toString(), dbConfigMap.get("password").toString());
                meta = DataBaseMetaFactory.getDataBaseMetaByType(BaseDataBaseMeta.TYPE_MYSQL, param);
            }
            if (tmap.containsKey("syncconfig")) {
                Map<String, Object> syncMap = (Map<String, Object>) tmap.get("syncconfig");
                Iterator<String> iter = syncMap.keySet().iterator();
                String[] tables = null;
                String[] types = null;
                while (iter.hasNext()) {
                    String key = iter.next();
                    Object obj = syncMap.get(key);

                    if (obj instanceof String) {
                        if (key.equalsIgnoreCase("tables")) {
                            tables = obj.toString().split(",");
                        } else if (key.equalsIgnoreCase("types")) {
                            types = obj.toString().split(",");
                        }
                    } else if (obj instanceof Map) {
                        Map<String, String> syncconfigMap = (Map<String, String>) obj;
                        String[] tableNames = syncconfigMap.get("tables").split(",");
                        types = syncconfigMap.get("types").split(",");
                        Map<String, Integer> tmpMap = new HashMap<String, Integer>();
                        for (String table : tableNames) {
                            List<Integer> operType = new ArrayList<Integer>();
                            for (String type : types) {
                                operType.add(translateType(type));
                            }
                            this.syncMap.put(table, operType);
                        }
                    }
                }
                if (tables != null) {
                    for (String table : tables) {
                        List<Integer> operType = new ArrayList<Integer>();
                        for (String type : types) {
                            operType.add(translateType(type));
                        }
                        this.syncMap.put(table, operType);
                    }
                }
            }
        }
    }

    public void getDbMeta() throws Exception {
        DataBaseUtil util = new DataBaseUtil();
        util.connect(meta);
        List<DataBaseTableMeta> list = util.getAllTable(meta.getParam().getDatabaseName(), meta);
        for (DataBaseTableMeta table : list) {
            if (!syncMap.isEmpty()) {
                if (syncMap.containsKey(table.getTableName().toUpperCase())) {
                    getColumnMeta(util, table);
                }
            } else {
                getColumnMeta(util, table);
            }
        }

    }

    public List<Integer> getAvaiableColumn(BitSet columnbit) {
        List<Integer> posList = new ArrayList<Integer>();
        for (int i = 0; i < columnbit.length(); i++) {
            if (columnbit.get(i)) {
                posList.add(i);
            }
        }
        return posList;
    }

    public void getColumnMeta(DataBaseUtil dbutil, DataBaseTableMeta table) throws Exception {
        List<DataBaseColumnMeta> columnList = dbutil.getTableMetaByTableName(table.getTableName(), meta.getParam().getDatabaseName(), meta);
        tableColumnMap.put(table.getTableName(), columnList);
        Map<Integer, DataBaseColumnMeta> columnMap = new HashMap<Integer, DataBaseColumnMeta>();
        for (int i = 0; i < columnList.size(); i++) {
            columnMap.put(i, columnList.get(i));
        }
        schemaMap.put(table.getTableName(), AvroUtils.getSchemaForDbMeta(dbNamespace, getClassNameByTableName(table.getTableName()), columnList));
        tableCfgMap.put(table.getTableName(), columnMap);
    }

    public String getClassNameByTableName(String tableName) {
        String tmpName = tableName.replaceAll("-", "");
        tmpName = tmpName.replaceAll("_", "");
        return tmpName.substring(0, 1).toUpperCase() + tmpName.substring(1, tmpName.length()) + "VO";
    }

    public Map<String, Object> retreiveRecord(String tableName, Serializable[] records, List<Integer> columnposList) {
        Map<String, Object> map = new HashMap<String, Object>();
        try {
            for (int i = 0; i < records.length; i++) {
                int pos = columnposList.get(i);
                DataBaseColumnMeta meta = tableColumnMap.get(tableName).get(pos);
                if(records[i] instanceof byte[]){
                    map.put(meta.getColumnName(),new String((byte[])records[i]));
                }else
                    map.put(meta.getColumnName(), records[i]);
            }
            map.put("dataSourceName",dataSourceName);
            map.put("tableName",tableName);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return map;
    }

    public static Integer translateType(String typeStr) {
        Integer type = 0;
        if (typeStr.equalsIgnoreCase("INSERT")) {
            type = 1;
        } else if (typeStr.equalsIgnoreCase("UPDATE")) {
            type = 2;
        } else if (typeStr.equalsIgnoreCase("DELETE")) {
            type = 3;
        }
        return type;
    }
}