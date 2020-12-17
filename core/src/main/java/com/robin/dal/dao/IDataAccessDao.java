package com.robin.dal.dao;

import com.robin.core.fileaccess.meta.DataCollectionMeta;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

public interface IDataAccessDao extends Closeable {

    int insertRecord(Map<String,Object> valueMap);
    List<Map<String,Object>> readRecords(String sql,String tsField,Object... params);
    void beforeExecute();

}
