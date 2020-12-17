package com.robin.dal.dao;


import com.robin.core.fileaccess.meta.DataMappingMeta;

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
    List<Map<String,Object>> executeQuerySync(DataMappingMeta mappingMeta,String sql,Object[] params);
    String executeQueryAsync(DataMappingMeta mappingMeta,String sql,Object[] params,Long writeOutSourceId,Map<String,String> configParams);


}
