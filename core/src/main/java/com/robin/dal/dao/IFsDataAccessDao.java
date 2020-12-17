package com.robin.dal.dao;

import com.robin.core.fileaccess.meta.DataCollectionMeta;

import javax.naming.OperationNotSupportedException;
import java.io.InputStream;
import java.io.OutputStream;

public interface IFsDataAccessDao extends IDataAccessDao {
    void init(DataCollectionMeta collectionMeta);
    void prepareRead(String path);
    void prepareWrite(String path) ;
    void finishRead(String path);
    void finishWrite(String path);
    InputStream getInputStream() throws OperationNotSupportedException;
    OutputStream getOutputStream() throws OperationNotSupportedException;

    void doImport(IDataAccessDao dataAccessDao);
    void doExport(OutputStream outputStream);
    void doUnCompress(OutputStream outputStream);


    void colease(int fileNum);
    void finish();
}
