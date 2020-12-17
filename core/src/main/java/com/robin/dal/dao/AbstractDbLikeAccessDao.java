package com.robin.dal.dao;

import com.robin.core.base.datameta.DataBaseColumnMeta;
import com.robin.core.base.datameta.DataBaseTableMeta;
import com.robin.core.base.datameta.DataBaseUtil;
import com.robin.core.base.exception.DAOException;
import com.robin.core.collection.util.CollectionMapConvert;
import com.robin.core.fileaccess.meta.DataMappingMeta;
import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public abstract class AbstractDbLikeAccessDao implements IBaseDbLikeAccessDao {
    protected final DataBaseTableMeta tableMeta;
    protected final List<DataBaseColumnMeta> columnList;
    protected final DataMappingMeta mappingMeta;
    protected Long resType;
    protected Map<String, Pair<String, String>> inputmappingMap = new HashMap<>();
    protected Map<String, Pair<String, String>> outputmappingMap = new HashMap<>();
    protected Map<String,DataBaseColumnMeta> tableColumnDefMap=new HashMap<>();
    protected List<String> pkColumns;
    protected Map<String,String> configParams;

    public AbstractDbLikeAccessDao(DataBaseTableMeta tableMeta, List<DataBaseColumnMeta> columnList,List<String> pkColumns, DataMappingMeta mappingMeta,Map<String,String> configParams) {
        this.tableMeta=tableMeta;
        this.columnList=columnList;
        this.pkColumns=pkColumns;
        columnList.stream().map(f->tableColumnDefMap.put(f.getColumnName(),f));
        //collectionMeta.getColumnList().parallelStream().map(f->tableColumnDefMap.put(f.getColumnName(),f));
        this.mappingMeta = mappingMeta;
        this.configParams=configParams;

    }



    protected void doMapping() throws Exception {
        if (mappingMeta == null || mappingMeta.getMappingMap() == null || mappingMeta.getMappingMap().isEmpty()) {
            if (columnList != null && !columnList.isEmpty()) {
                for (DataBaseColumnMeta columnMeta : columnList) {
                    outputmappingMap.put(columnMeta.getColumnName(), new Pair<>(getPropertyNameByColumnName(columnMeta.getColumnName()), columnMeta.getColumnType().toString()));
                    inputmappingMap.put(getPropertyNameByColumnName(columnMeta.getColumnName()), new Pair<>(columnMeta.getColumnName(), columnMeta.getColumnType().toString()));
                }
            }
        } else {
            Map<String, List<DataBaseColumnMeta>> columnMap = CollectionMapConvert.convertToMapByParentKey(columnList, "columnName");
            Iterator<Map.Entry<String, String>> iterator = mappingMeta.getMappingMap().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                if (columnMap.containsKey(entry.getKey())) {
                    outputmappingMap.put(entry.getKey(), new Pair<>(entry.getValue(), columnMap.get(entry.getKey()).get(0).getColumnType().toString()));
                    inputmappingMap.put(entry.getValue(), new Pair<>(entry.getKey(), columnMap.get(entry.getKey()).get(0).getColumnType().toString()));
                }
            }
        }
    }

    protected String getPropertyNameByColumnName(String columnName) {
        StringTokenizer tokenizer = new StringTokenizer(columnName, "-_");
        StringBuilder builder = new StringBuilder();
        while (tokenizer.hasMoreElements()) {
            if (builder.length() == 0) {
                builder.append(tokenizer.nextToken());
            } else {
                String token = tokenizer.nextToken().toLowerCase();
                builder.append(token.substring(0, 1).toUpperCase() + token.substring(1).toLowerCase());
            }
        }
        if (builder.length() > 0) {
            return builder.toString();
        } else {
            return columnName;
        }
    }

    protected String generateInsertSql(String schema, String tableName) {
        String insertName = schema != null && !schema.isEmpty() ? schema + "." + tableName : tableName;
        return new StringBuilder("insert into ").append(insertName).append(generateInsertPart()).toString();
    }

    protected String generateUpdateSql(String schema, String tableName) {
        String updateName = schema != null && !schema.isEmpty() ? schema + "." + tableName : tableName;
        return new StringBuilder("update ").append(updateName).append(" set ").append(generateInsertPart()).toString();
    }

    protected String generateDeleteSql(String schema, String tableName){
        String delName = schema != null && !schema.isEmpty() ? schema + "." + tableName : tableName;
        return new StringBuilder("delete from ").append(delName).append(generateDeletePart()).toString();
    }

    protected String generateInsertPart() {
        StringBuilder insertbuilder = new StringBuilder("(");
        StringBuilder valueBuilder = new StringBuilder("(");
        if (mappingMeta.getMappingMap() == null) {
            for (DataBaseColumnMeta columnMeta : columnList) {
                if(!columnMeta.isIncrement()) {
                    insertbuilder.append(columnMeta.getColumnName()).append(",");
                    valueBuilder.append("?,");
                }
            }
        } else {
            for (DataBaseColumnMeta columnMeta : columnList) {
                if (mappingMeta.getMappingMap().containsKey(columnMeta.getColumnName()) && !tableColumnDefMap.get(columnMeta.getColumnName()).isIncrement()) {
                    insertbuilder.append(columnMeta.getColumnName()).append(",");
                    valueBuilder.append("?,");
                }
            }
        }
        return new StringBuilder(insertbuilder.substring(0, insertbuilder.length() - 1)).append(") values ")
                .append(valueBuilder.substring(0, valueBuilder.length() - 1)).append(")").toString();
    }

    protected String generateUpdatePart() throws DAOException{
        StringBuilder builder = new StringBuilder();
        StringBuilder wherebuilder = new StringBuilder(" where ");
        if(pkColumns==null || pkColumns.isEmpty()){
            throw new DAOException("pk column must exist in table,Otherwise can not update");
        }
        if (mappingMeta.getMappingMap() == null) {
            for (DataBaseColumnMeta columnMeta : columnList) {
                if (!pkColumns.contains(columnMeta.getColumnName())) {
                    builder.append(columnMeta.getColumnName()).append("=?,");
                }
            }
            for (String pkColumn : pkColumns) {
                wherebuilder.append(pkColumn).append("=?,");
            }
        } else {
            for (DataBaseColumnMeta columnMeta : columnList) {
                if (mappingMeta.getMappingMap().containsKey(columnMeta.getColumnName())) {
                    if (!pkColumns.contains(columnMeta.getColumnName())) {
                        builder.append(columnMeta.getColumnName()).append("=?,");
                    }
                }
            }
            for (String pkColumn : pkColumns) {
                if (!mappingMeta.getMappingMap().containsKey(pkColumn)) {
                    throw new DAOException("pk column must exist in mapping,Otherwise can not update");
                }
                wherebuilder.append(pkColumn).append("=?,");
            }
        }
        return new StringBuilder(builder.substring(0, builder.length() - 1)).append(wherebuilder.substring(0, wherebuilder.length() - 1)).toString();
    }

    protected String generateDeletePart() throws DAOException{
        StringBuilder builder = new StringBuilder(" where ");
        if(pkColumns==null || pkColumns.isEmpty()){
            throw new DAOException("pk column must exist in table,Otherwise can not update");
        }
        if (mappingMeta.getMappingMap() == null) {
            for (String pkColumn : pkColumns) {
                builder.append(pkColumn).append("=?,");
            }
        } else {
            for (String pkColumn : pkColumns) {
                if (!mappingMeta.getMappingMap().containsKey(pkColumn)) {
                    throw new DAOException("pk column must exist in mapping,Otherwise can not update");
                }
                builder.append(pkColumn).append("=?,");
            }
        }
        return builder.substring(0,builder.length()-1);
    }
    protected Object[] wrapInsertPerpareStamtementParams(Map<String,Object> valueMap){
        List<Object> retObjs=new ArrayList<>();
        try {
            if (mappingMeta.getMappingMap() == null) {
                for (DataBaseColumnMeta columnMeta : columnList) {
                    if(!columnMeta.isIncrement())
                        retObjs.add(DataBaseUtil.translateValueByDBType(valueMap.get(columnMeta.getColumnName()).toString(), columnMeta.getColumnType().toString()));
                }
            } else {
                for (DataBaseColumnMeta columnMeta : columnList) {
                    if (mappingMeta.getMappingMap().containsKey(columnMeta.getColumnName()) && !tableColumnDefMap.get(columnMeta.getColumnName()).isIncrement()) {
                        retObjs.add(DataBaseUtil.translateValueByDBType(valueMap.get(mappingMeta.getMappingMap().get(columnMeta.getColumnName())).toString(), columnMeta.getColumnType().toString()));
                    }
                }
            }
        }catch (Exception ex){
            log.error("",ex);
        }
        return retObjs.toArray();
    }
    protected Object[] wrapUpdatePerpareStamtementParams(Map<String,Object> valueMap) throws Exception{
        List<Object> retObjs=new ArrayList<>();
        if (mappingMeta.getMappingMap() == null) {
            for (DataBaseColumnMeta columnMeta : columnList) {
                if(!pkColumns.contains(columnMeta.getColumnName())) {
                    retObjs.add(DataBaseUtil.translateValueByDBType(valueMap.get(columnMeta.getColumnName()).toString(), columnMeta.getColumnType().toString()));
                }
            }
            for(String pkColumn:pkColumns){
                retObjs.add(DataBaseUtil.translateValueByDBType(valueMap.get(pkColumn).toString(),tableColumnDefMap.get(pkColumn).getColumnType().toString()));
            }

        } else {
            for (DataBaseColumnMeta columnMeta : columnList) {
                if (mappingMeta.getMappingMap().containsKey(columnMeta.getColumnName())) {
                    if(!pkColumns.contains(columnMeta.getColumnName())) {
                        retObjs.add(DataBaseUtil.translateValueByDBType(valueMap.get(mappingMeta.getMappingMap().get(columnMeta.getColumnName())).toString(), columnMeta.getColumnType().toString()));
                    }
                }
                for(String pkColumn:pkColumns){
                    retObjs.add(DataBaseUtil.translateValueByDBType(valueMap.get(mappingMeta.getMappingMap().get(pkColumn)).toString(),tableColumnDefMap.get(pkColumn).getColumnType().toString()));
                }
            }
        }
        return retObjs.toArray();
    }

}
