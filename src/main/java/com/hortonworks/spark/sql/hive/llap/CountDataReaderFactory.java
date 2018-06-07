package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class CountDataReaderFactory implements DataReaderFactory<ColumnarBatch> {
  private long numRows;

  public CountDataReaderFactory(long numRows) {
    this.numRows = numRows;
  }

  @Override
  public DataReader<ColumnarBatch> createDataReader() {
    return new CountDataReader(numRows);
  }
}
