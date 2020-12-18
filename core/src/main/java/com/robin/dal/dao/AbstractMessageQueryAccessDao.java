package com.robin.dal.dao;

import com.robin.core.fileaccess.iterator.AbstractResIterator;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.writer.AbstractQueueWriter;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractMessageQueryAccessDao implements IMessageQueueAccessDao {
    protected String messageType;
    protected DataCollectionMeta collectionMeta;
    protected AbstractQueueWriter writer;
    protected AbstractResIterator iterator;
    protected String queueName;

    @Override
    public void init(DataCollectionMeta collectionMeta) {
        this.collectionMeta=collectionMeta;
        if(!CollectionUtils.isEmpty(collectionMeta.getResourceCfgMap())){
            queueName=collectionMeta.getResourceCfgMap().get("queueName").toString();
        }
    }

    @Override
    public int insertRecord(Map<String, Object> valueMap) throws IOException {
        throw new IOException("operation not supported");
    }
}
