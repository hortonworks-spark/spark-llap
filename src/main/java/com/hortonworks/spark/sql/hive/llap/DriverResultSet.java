package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;


import java.util.List;

//Holder class for data return directly to the Driver from HS2
public class DriverResultSet {

    public DriverResultSet(List<Row> data, StructType schema) {
       this.data = data;
       this.schema = schema;
    }

    public List<Row> data;
    public StructType schema;

    public Dataset<Row> asDataFrame(SparkSession session) {
      return session.createDataFrame(data, schema);
    }
}
