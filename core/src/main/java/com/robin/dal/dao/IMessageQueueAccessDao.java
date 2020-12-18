package com.robin.dal.dao;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Message Queue based data access DAO
 */
public interface IMessageQueueAccessDao extends IDataAccessDao {
    boolean insertMessage(String queue,Map<String,Object> valueMap) throws IOException;
    List<Map<String,Object>> pollMessage(String queue);
}
