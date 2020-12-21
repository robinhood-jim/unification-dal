package com.robin.dal.dao.jdbc;

import com.robin.comm.dal.holder.db.DbConnectionHolder;
import com.robin.comm.dal.pool.ResourceAccessHolder;
import com.robin.core.base.dao.SimpleJdbcDao;
import com.robin.core.base.datameta.BaseDataBaseMeta;
import com.robin.core.base.datameta.DataBaseColumnMeta;
import com.robin.core.base.datameta.DataBaseTableMeta;
import com.robin.core.base.spring.SpringContextHolder;
import com.robin.core.fileaccess.meta.DataMappingMeta;
import com.robin.core.query.extractor.ResultSetOperationExtractor;
import com.robin.dal.dao.AbstractDbLikeAccessDao;
import com.robin.dal.dao.IBaseDbLikeAccessDao;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
@Slf4j
public class JdbcAccessDao extends AbstractDbLikeAccessDao implements IBaseDbLikeAccessDao {

    private DbConnectionHolder connectionHolder;
    private BaseDataBaseMeta meta;
    private Long sourceId;
    private ResultSet resultSet;

    public JdbcAccessDao(DataBaseTableMeta tableMeta, List<DataBaseColumnMeta> columnList,List<String> pkColumns, DataMappingMeta mappingMeta, BaseDataBaseMeta meta, Long sourceId) throws Exception {
        super(tableMeta,columnList,pkColumns,mappingMeta,null);
        this.meta=meta;
        connectionHolder= SpringContextHolder.getBean(ResourceAccessHolder.class).getConnectionHolder(sourceId,meta);

        doMapping();
    }

    @Override
    public int insertRecord(Map<String, Object> valueMap) throws IOException {
        Connection connection=null;
        int ret=0;
        try {
            connection=connectionHolder.getConnection();
            String insertSql=generateInsertSql(tableMeta.getSchema(),tableMeta.getTableName());
            ret=SimpleJdbcDao.executeUpdate(connection,insertSql,wrapInsertPerpareStamtementParams(valueMap));
        }catch (Exception ex){
            throw new IOException(ex);
        }finally {
            if(connection!=null){
                connectionHolder.closeConnection(connection);
            }
        }
        return ret;
    }


    @Override
    public int batchRecord(List<Map<String, Object>> valueList) {
        Connection connection=null;
        int ret=0;
        try{
            connection=connectionHolder.getConnection();
            connection.setAutoCommit(false);
            List<Object[]> parmaList=new ArrayList<>();
            String insertSql=generateInsertSql(tableMeta.getSchema(),tableMeta.getTableName());
            valueList.parallelStream().map(f->parmaList.add(wrapInsertPerpareStamtementParams(f)));
            ret=SimpleJdbcDao.simpleBatch(connection,insertSql,parmaList);
            connection.commit();
        }catch (Exception ex){
            try {
                if (connection != null) {
                    connection.rollback();
                    connectionHolder.closeConnection(connection);
                }
            }catch (Exception e){

            }
        }
        return ret;
    }

    @Override
    public boolean updateRecord(Map<String, Object> valueMap) {
        Connection connection=null;
        int ret=0;
        try {
            connection=connectionHolder.getConnection();
            String insertSql=generateUpdateSql(tableMeta.getSchema(),tableMeta.getTableName());
            ret=SimpleJdbcDao.executeUpdate(connection,insertSql,wrapInsertPerpareStamtementParams(valueMap));
        }catch (Exception ex){
            log.error("",ex);
        }finally {
            if(connection!=null){
                connectionHolder.closeConnection(connection);
            }
        }
        return ret>0;
    }

    @Override
    public boolean deleteRecord(Map<String, Object> valueMap) {
        return false;
    }

    @Override
    public List<Map<String, String>> executeQuerySync(String sql, Object[] params) throws SQLException {
        Connection connection=null;
        try {
            connection=connectionHolder.getConnection();
            return SimpleJdbcDao.queryBySql(connection,sql,params);
        }catch (Exception ex){
            throw new SQLException(ex);
        }finally {
            if(connection!=null){
                connectionHolder.closeConnection(connection);
            }
        }
    }
    @Override
    public void doOperationInQuery(ResultSetOperationExtractor extractor, String sql, Object... params) throws SQLException{
        Connection connection=null;
        try {
            connection=connectionHolder.getConnection();
            SimpleJdbcDao.executeOperationWithQuery(connection,sql,params,extractor);
        }catch (SQLException ex){
            throw ex;
        }finally {
            if(connection!=null){
                connectionHolder.closeConnection(connection);
            }
        }
    }


    @Override
    public void close() {
        if(connectionHolder.canClose()){
            connectionHolder.close();
        }
    }

    @Override
    public String executeQueryAsync(DataMappingMeta mappingMeta, String sql, Object[] params, Long writeOutSourceId, Map<String, String> configParams) {
        return null;
    }

    @Override
    public void beforeExecute() {

    }
}
