package com.robin.dal.comm;

public class Constant {
    public enum DAL_PROCESS_STATUS{
        READ("1"),
        WRITE("2");
        DAL_PROCESS_STATUS(String value){
            this.value=value;
        }
        private String value;
        public String getValue(){
            return this.value;
        }
    }
}
