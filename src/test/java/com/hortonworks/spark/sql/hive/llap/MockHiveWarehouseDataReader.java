package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import java.io.IOException;

public class MockHiveWarehouseDataReader implements DataReader<Row> {

    private static final int RESULT_SIZE = 10;
    private int i = 0;

    @Override
    public boolean next() throws IOException {
        return (i < 10);
    }

    @Override
    public Row get() {
        Row value = new GenericRow(new Object[] {i, "Element " + i});
        i++;
        return value;
    }

    @Override
    public void close() {

    }
}
