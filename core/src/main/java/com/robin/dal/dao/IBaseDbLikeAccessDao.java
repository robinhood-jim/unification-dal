package com.robin.dal.dao;


import com.robin.core.fileaccess.meta.DataMappingMeta;
import com.robin.core.query.extractor.ResultSetOperationExtractor;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * <p>Created at: 2019-09-19 13:51:18</p>
 *
 * @author robinjim
 * @version 1.0
 */
public interface IBaseDbLikeAccessDao extends IDataAccessDao {
    boolean updateRecord(Map<String,Object> valueMap);
    boolean deleteRecord(Map<String,Object> valueMap);
    int batchRecord(List<Map<String,Object>> valueList);
    //void flushDataToFs(DataMappingMeta mappingMeta, String sql, Object[] params, RecordWriterHolder holder);
    List<Map<String,String>> executeQuerySync(String sql,Object[] params) throws SQLException;
    String executeQueryAsync(DataMappingMeta mappingMeta,String sql,Object[] params,Long writeOutSourceId,Map<String,String> configParams);
    void doOperationInQuery(ResultSetOperationExtractor extractor, String sql, Object... params) throws SQLException;

}
