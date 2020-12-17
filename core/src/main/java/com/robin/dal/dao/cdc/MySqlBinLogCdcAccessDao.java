package com.robin.dal.dao.cdc;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.robin.core.base.datameta.BaseDataBaseMeta;
import com.robin.core.fileaccess.writer.IResourceWriter;
import com.robin.dal.dao.ICdcIncrementAccessDao;
import org.apache.commons.io.FileUtils;
import org.springframework.util.Assert;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>Created at: 2019-09-21 10:41:33</p>
 *
 * @author robinjim
 * @version 1.0
 */
public class MySqlBinLogCdcAccessDao implements ICdcIncrementAccessDao {
    private BaseDataBaseMeta metaData;
    private BinaryLogClient client;
    private IResourceWriter writer;
    private Long logPos;
    private String binlogFileName;
    private LoadingCache<Long, Integer> cache;
    private String restoreFile="binlog.cfg";
    private boolean runTag=false;

    @Override
    public void init(BaseDataBaseMeta metaData, Map<String, Object> configMap) {
        this.metaData=metaData;
        Assert.isTrue(metaData.getDbType().equals(BaseDataBaseMeta.TYPE_MYSQL),"Only support Mysql");
        client = new BinaryLogClient(metaData.getParam().getHostName(), metaData.getParam().getPort(), metaData.getParam().getUserName(), metaData.getParam().getPasswd());
        restoreFromFile();
        if(null!=logPos &&  logPos!=0L){
            client.setBinlogPosition(logPos);
            client.setBinlogFilename(binlogFileName);
        }
        cache = CacheBuilder.newBuilder().initialCapacity(1000).expireAfterAccess(30, TimeUnit.MINUTES).removalListener((RemovalListener<Long, Integer>) removalNotification -> {}).build(new CacheLoader<Long, Integer>() {
            @Override
            public Integer load(Long longval) throws Exception {
                return 0;
            }
        });

    }
    private void restoreFromFile(){
        String tmpPath=System.getProperty("java.io.tmpdir");
        File file=new File(tmpPath+"/"+restoreFile);
        try {
            if (file.exists()) {
                List<String> lines = FileUtils.readLines(file);
                lines.forEach(f->{
                    if(f.startsWith("logPos=")){
                        logPos=Long.valueOf(f.substring(8));
                    }else if(f.startsWith("logFile=")){
                        binlogFileName=f.substring(9);
                    }
                });
            }
        }catch (Exception ex){

        }
    }
    private void saveLastAccess(){
        BufferedWriter writer=null;
        if (!runTag) {
            try {
                String tmpPath=System.getProperty("java.io.tmpdir");
                File file=new File(tmpPath+"/"+restoreFile);
                writer=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
                writer.write("logPos="+logPos+"\n");
                writer.write("logFile="+binlogFileName);
                writer.flush();
            } catch (Exception ex) {

            }finally {
                try {
                    if (null != writer) {
                        writer.close();
                    }
                }catch (Exception ex){

                }
            }
        }
    }


    @Override
    public void beginTrace() {

    }

    @Override
    public void stopTrace() {

    }

    @Override
    public boolean flushToFs(IResourceWriter writer) {
        return false;
    }

}
