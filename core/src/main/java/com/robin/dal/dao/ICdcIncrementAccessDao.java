package com.robin.dal.dao;

import com.robin.core.base.datameta.BaseDataBaseMeta;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.writer.IResourceWriter;


import java.util.Map;

/**
 * <p>Created at: 2019-09-21 09:27:04</p>
 *
 * @author robinjim
 * @version 1.0
 */
public interface ICdcIncrementAccessDao {
    void init(BaseDataBaseMeta dataBaseMeta, Map<String,Object> configMap);
    void beginTrace();
    void stopTrace();
    boolean flushToFs(IResourceWriter writer);

}
