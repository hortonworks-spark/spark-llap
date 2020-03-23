package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class CountDataReaderFactory implements InputPartition<ColumnarBatch> {
  private long numRows;

  public CountDataReaderFactory(long numRows) {
    this.numRows = numRows;
  }

  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    return new CountDataReader(numRows);
  }
}
