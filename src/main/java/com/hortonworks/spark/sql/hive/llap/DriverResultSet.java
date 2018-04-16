package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.types.StructType;

import java.util.List;

public class DriverResultSet {

    public DriverResultSet(List<?> data, StructType schema) {
       this.data = data;
       this.schema = schema;
    }

    public List<?> data;
    public StructType schema;
}
