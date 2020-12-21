package com.robin.dal.dao;

import com.robin.core.fileaccess.meta.DataCollectionMeta;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface IFsDataAccessDao extends IDataAccessDao {

    void prepareRead(String path) throws IOException;
    void prepareReadRows(String path) throws IOException;
    void prepareWrite(String path) throws IOException;
    void prepareWriteRows(String path) throws IOException;
    void finishRead() throws IOException;
    void finishWrite() throws IOException;
    InputStream getInputStream() throws OperationNotSupportedException;
    OutputStream getOutputStream() throws OperationNotSupportedException;

    void doImport(IDataAccessDao dataAccessDao,String targetPath);
    void doExport(String sourcePath,IDataAccessDao targetDao);
    void doUnCompress(OutputStream outputStream);


    void colease(int fileNum);
    void finish();
}
