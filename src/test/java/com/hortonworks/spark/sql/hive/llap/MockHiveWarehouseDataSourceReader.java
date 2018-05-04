package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class MockHiveWarehouseDataSourceReader implements DataSourceReader {

    @Override
    public StructType readSchema() {
       return (new StructType())
               .add("col1", "int")
               .add("col2", "string");
    }

    @Override
    public List<DataReaderFactory<Row>> createDataReaderFactories() {
        return Arrays.asList(new MockHiveWarehouseDataReaderFactory());
    }
}
