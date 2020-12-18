package com.robin.dal.dao.queue;

import com.robin.comm.resaccess.writer.KafkaResourceWriter;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.dal.dao.AbstractMessageQueryAccessDao;
import com.robin.dal.dao.IMessageQueueAccessDao;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KafkaAccessDao extends AbstractMessageQueryAccessDao implements IMessageQueueAccessDao {
    private Map<String,Object> configMap=null;

    @Override
    public void init(DataCollectionMeta collectionMeta) {
        configMap=collectionMeta.getResourceCfgMap();
        if(!CollectionUtils.isEmpty(collectionMeta.getResourceCfgMap())){
            writer=new KafkaResourceWriter(collectionMeta);
        }
    }

    @Override
    public void beforeExecute() {

    }

    @Override
    public boolean insertMessage(String queue, Map<String, Object> valueMap) throws IOException {
        writer.writeMessage(queue,valueMap);
        return false;
    }

    @Override
    public List<Map<String, Object>> pollMessage(String queue) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
