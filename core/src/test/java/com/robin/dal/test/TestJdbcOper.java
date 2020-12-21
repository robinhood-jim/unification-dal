package com.robin.dal.test;

import com.robin.core.base.dao.SimpleJdbcDao;
import com.robin.core.base.datameta.*;
import com.robin.core.fileaccess.meta.DataMappingMeta;
import com.robin.dal.dao.jdbc.JdbcAccessDao;
import junit.framework.TestCase;
import org.apache.commons.dbutils.DbUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Created at: 2019-09-21 15:43:51</p>
 *
 * @author robinjim
 * @version 1.0
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:applicationContext-test.xml")
public class TestJdbcOper extends TestCase {
    @Test
    public void testInsert(){
        DataBaseParam param=new DataBaseParam("127.0.0.1",3316,"test","root","root");
        BaseDataBaseMeta meta= DataBaseMetaFactory.getDataBaseMetaByType(BaseDataBaseMeta.TYPE_MYSQL,param);
        Connection conn=null;
        try{
            conn= SimpleJdbcDao.getConnection(meta,param);
            List<DataBaseColumnMeta> columnMetaList= DataBaseUtil.getTableMetaByTableName(conn,"t_test","test",meta.getDbType());
            DataBaseTableMeta tableMeta=new DataBaseTableMeta("test","t_test");
            Map<String,Object> vMap=new HashMap<>();
            vMap.put("id",123123);
            vMap.put("name","ttteset");
            vMap.put("code_desc","2213123");
            vMap.put("cs_id",2);
            JdbcAccessDao accessDao=new JdbcAccessDao(tableMeta,columnMetaList, Collections.singletonList("id"),new DataMappingMeta(),meta,1L);
            accessDao.insertRecord(vMap);


        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            if(conn!=null){
                DbUtils.closeQuietly(conn);
            }
        }

    }
}
