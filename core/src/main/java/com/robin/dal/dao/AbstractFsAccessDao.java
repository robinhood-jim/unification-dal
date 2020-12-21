package com.robin.dal.dao;

import com.robin.core.base.util.IOUtils;
import com.robin.core.base.util.StringUtils;
import com.robin.core.fileaccess.iterator.AbstractResIterator;
import com.robin.core.fileaccess.iterator.TextFileIteratorFactory;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.core.fileaccess.util.AbstractResourceAccessUtil;
import com.robin.core.fileaccess.writer.IResourceWriter;
import com.robin.core.fileaccess.writer.TextFileWriterFactory;
import com.robin.dal.comm.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public abstract class AbstractFsAccessDao implements IFsDataAccessDao {
    protected DataCollectionMeta collectionMeta;
    protected Map<String, DataSetColumnMeta> columnMap;
    protected IResourceWriter writer;
    protected OutputStream outputStream;
    protected InputStream inputStream;
    protected AbstractResIterator iterator;
    protected AbstractResourceAccessUtil util;
    protected String processPath;
    protected String status;
    protected Integer batchSize=1000;
    //信号量
    protected AtomicInteger runningTag=new AtomicInteger(1);

    @Override
    public void init(DataCollectionMeta collectionMeta) {
        Assert.notNull(collectionMeta,"");
        this.collectionMeta=collectionMeta;
        if(!CollectionUtils.isEmpty(collectionMeta.getResourceCfgMap()) &&
                collectionMeta.getResourceCfgMap().containsKey("data.batchSize") && !StringUtils.isEmpty(collectionMeta.getResourceCfgMap().get("data.batchSize"))){
            batchSize=Integer.parseInt(collectionMeta.getResourceCfgMap().get("data.batchSize").toString());
        }
        collectionMeta.getColumnList().stream().map(f->columnMap.put(f.getColumnName(),f));
    }

    @Override
    public void prepareRead(String path) throws IOException {
        //等待信号量为1，运行任务完成
        while (!this.runningTag.compareAndSet(1,0)){
            LockSupport.parkNanos(1L);
        }
        this.inputStream=util.getInResourceByStream(this.collectionMeta,path);
        this.status= Constant.DAL_PROCESS_STATUS.READ.getValue();
    }

    @Override
    public void prepareReadRows(String path) throws IOException {
        prepareRead(path);
        this.iterator= TextFileIteratorFactory.getProcessIteratorByType(collectionMeta,inputStream);
    }

    @Override
    public void prepareWriteRows(String path) throws IOException {
        prepareWrite(path);
        this.writer= TextFileWriterFactory.getFileWriterByPath(collectionMeta,outputStream);
    }

    @Override
    public void prepareWrite(String path) throws IOException {
        //等待信号量为1，运行任务完成
        while (!this.runningTag.compareAndSet(1,0)){
            LockSupport.parkNanos(1L);
        }
        this.outputStream=util.getOutResourceByStream(this.collectionMeta,path);
        this.status= Constant.DAL_PROCESS_STATUS.READ.getValue();
    }

    @Override
    public void finishRead() {
        try{
            if(null!=iterator){
                iterator.close();
            }
            if(null!=inputStream){
                inputStream.close();
            }
        }catch (Exception ex){
            log.error("{}",ex);
        }finally {
            //重置标志
            while (!this.runningTag.compareAndSet(0,1)){
                LockSupport.parkNanos(1L);
            }
            status=null;
        }
    }

    @Override
    public InputStream getInputStream() throws OperationNotSupportedException {
        if(null==status || !Constant.DAL_PROCESS_STATUS.READ.getValue().equals(status)){
            throw new OperationNotSupportedException("accessor not initialize or still in write!");
        }
        return inputStream;
    }

    @Override
    public OutputStream getOutputStream() throws OperationNotSupportedException{
        if(null==status || !Constant.DAL_PROCESS_STATUS.WRITE.getValue().equals(status)){
            throw new OperationNotSupportedException("accessor not initialize or still in read!");
        }
        return outputStream;
    }

    @Override
    public void finishWrite() {
        try{
            if(null!=writer){
                writer.close();
            }
            if(null!=outputStream){
                outputStream.close();
            }
        }catch (Exception ex){
            log.error("{}",ex);
        }finally {
            //重置标志
            while (!this.runningTag.compareAndSet(0,1)){
                LockSupport.parkNanos(1L);
            }
        }
        status=null;
    }

    @Override
    public void doExport(String sourcePath,IDataAccessDao targetDao) {
        Assert.notNull(targetDao,"target source must not be null");
        Assert.notNull(util,"");
        try {
            //FileSystem Export,using Stream
            if (targetDao.getClass().isAssignableFrom(AbstractFsAccessDao.class)) {
                AbstractFsAccessDao fsDao = (AbstractFsAccessDao) targetDao;
                Assert.notNull(fsDao.getAccessUtil(), "");
                OutputStream outputStream = fsDao.getOutputStream();
                InputStream inputStream=this.getInputStream();
                IOUtils.copyBytes(inputStream,outputStream,8192);
            } else if (targetDao.getClass().isAssignableFrom(AbstractDbLikeAccessDao.class)) {
                //To Db like repository,using iterator
                List<Map<String,Object>> rsList=new ArrayList<>();
                AbstractDbLikeAccessDao dbDao=(AbstractDbLikeAccessDao) targetDao;
                int processRow=1;
                if(null!=iterator){
                    while(iterator.hasNext()){
                        Map<String,Object> valueMap=iterator.next();
                        rsList.add(valueMap);
                        if(processRow % batchSize==0){
                            dbDao.batchRecord(rsList);
                            rsList.clear();
                        }
                    }
                    processRow++;
                }
            }
        }catch (Exception ex){

        }
    }

    @Override
    public void doImport(IDataAccessDao dataAccessDao, String targetPath) {

    }

    public AbstractResourceAccessUtil getAccessUtil() {
        return util;
    }

    public DataCollectionMeta getCollectionMeta() {
        return collectionMeta;
    }

    @Override
    public void close() throws IOException {
        if(null!=status){
            if(Constant.DAL_PROCESS_STATUS.READ.getValue().equals(status)){
                finishRead();
            }else if(Constant.DAL_PROCESS_STATUS.WRITE.getValue().equals(status)){
                finishWrite();
            }
        }
    }
}
