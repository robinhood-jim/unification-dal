package com.robin.dal.dao;

import com.robin.core.fileaccess.iterator.AbstractResIterator;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.core.fileaccess.util.AbstractResourceAccessUtil;
import com.robin.core.fileaccess.writer.IResourceWriter;
import com.robin.dal.comm.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import javax.naming.OperationNotSupportedException;
import java.io.InputStream;
import java.io.OutputStream;
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
    //信号量
    protected AtomicInteger runningTag=new AtomicInteger(1);

    @Override
    public void init(DataCollectionMeta collectionMeta) {
        Assert.notNull(collectionMeta,"");
        this.collectionMeta=collectionMeta;
        collectionMeta.getColumnList().stream().map(f->columnMap.put(f.getColumnName(),f));
    }

    @Override
    public void prepareRead(String path) {
        //等待信号量为1，运行任务完成
        while (!this.runningTag.compareAndSet(1,0)){
            LockSupport.parkNanos(1L);
        }
        this.status= Constant.DAL_PROCESS_STATUS.READ.getValue();
    }

    @Override
    public void prepareWrite(String path) {
        //等待信号量为1，运行任务完成
        while (!this.runningTag.compareAndSet(1,0)){
            LockSupport.parkNanos(1L);
        }
        this.status= Constant.DAL_PROCESS_STATUS.READ.getValue();
    }

    @Override
    public void finishRead(String path) {
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
    public void finishWrite(String path) {
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
}
