package com.robin.dal.dao.fs;

import com.robin.comm.dal.pool.ResourceAccessHolder;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.dal.dao.AbstractFsAccessDao;
import com.robin.dal.dao.IDataAccessDao;
import com.robin.dal.dao.IFsDataAccessDao;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class HdfsDataAccessDao extends AbstractFsAccessDao implements IFsDataAccessDao {
    @Override
    public void init(DataCollectionMeta collectionMeta) {
        super.init(collectionMeta);
        try {
            util = ResourceAccessHolder.getAccessUtilByProtocol("hdfs");

        }catch (Exception ex){

        }
    }

    @Override
    public void doImport(IDataAccessDao dataAccessDao) {

    }

    @Override
    public void doExport(OutputStream outputStream) {

    }

    @Override
    public void doUnCompress(OutputStream outputStream) {

    }

    @Override
    public int insertRecord(Map<String, Object> valueMap) {
        return 0;
    }

    @Override
    public List<Map<String, Object>> readRecords(String sql, String tsField, Object... params) {
        return null;
    }

    @Override
    public void beforeExecute() {

    }


    @Override
    public void colease(int size) {

    }

    @Override
    public void finish() {

    }

    @Override
    public void close() throws IOException {

    }
}
