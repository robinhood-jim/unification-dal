package com.robin.dal.dao.fs;

import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.util.ApacheVfsResourceAccessUtil;
import com.robin.dal.dao.AbstractFsAccessDao;
import com.robin.dal.dao.IDataAccessDao;
import com.robin.dal.dao.IFsDataAccessDao;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

@Slf4j
public class VfsFsDataAccessDao extends AbstractFsAccessDao implements IFsDataAccessDao {
    private StandardFileSystemManager manager;

    @Override
    public void init(DataCollectionMeta collectionMeta) {
        super.init(collectionMeta);
        try {
            manager = new StandardFileSystemManager();
            manager.init();
            util=new ApacheVfsResourceAccessUtil();
        } catch (Exception ex) {
            log.error("", ex);
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
    public void colease(int fileNum) {

    }

    @Override
    public void finish() {

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
    public void close() throws IOException {

    }
}
