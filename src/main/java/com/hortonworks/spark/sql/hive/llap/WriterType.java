package com.hortonworks.spark.sql.hive.llap;

public enum WriterType {
    DELIMITED,
    JSON;

    public static WriterType parse(String writerTypeName){
        WriterType type;
        if(writerTypeName.equals("delimited")) {
            type = DELIMITED;
        } else if(writerTypeName.equals("json")) {
            type = JSON;
        } else {
            throw new IllegalArgumentException("Use either delimited or json");
        }
        return type;
    }
}