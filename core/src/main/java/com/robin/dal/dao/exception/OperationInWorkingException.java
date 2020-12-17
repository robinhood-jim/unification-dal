package com.robin.dal.dao.exception;

public class OperationInWorkingException extends RuntimeException {
    public OperationInWorkingException(Exception ex){
        super(ex);
    }
    public OperationInWorkingException(String message){
        super(message);
    }
}
