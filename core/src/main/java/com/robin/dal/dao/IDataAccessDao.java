package com.robin.dal.dao;

import com.robin.core.fileaccess.meta.DataCollectionMeta;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface IDataAccessDao extends Closeable {
    void init(DataCollectionMeta collectionMeta);
    int insertRecord(Map<String,Object> valueMap) throws IOException;
    void beforeExecute();

}
