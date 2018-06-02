package com.hortonworks.spark.sql.hive.llap;

import org.apache.arrow.vector.FieldVector;
import org.apache.hadoop.hive.llap.LlapArrowBatchRecordReader;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class HiveWarehouseDataReader implements DataReader<ColumnarBatch> {

  private LlapArrowBatchRecordReader reader;
  private ArrowWrapperWritable wrapperWritable = new ArrowWrapperWritable();
  private long arrowAllocatorMax;
  private static AtomicLong ids = new AtomicLong(0);
  private long id;

  public HiveWarehouseDataReader(LlapInputSplit split, JobConf conf, long arrowAllocatorMax) throws Exception {
    LlapBaseInputFormat input = new LlapBaseInputFormat(true, arrowAllocatorMax);
    this.reader = (LlapArrowBatchRecordReader) input.getRecordReader(split, conf, null);
    this.id = ids.getAndIncrement();
  }

  @Override public boolean next() throws IOException {
    boolean hasNextBatch = reader.next(null, wrapperWritable);
    return hasNextBatch;
  }

  @Override public ColumnarBatch get() {
    //Spark asks you to convert one column at a time so that different
    //column types can be handled differently.
    //NumOfCols << NumOfRows so this is negligible
    List<FieldVector> fieldVectors = wrapperWritable.getVectorSchemaRoot().getFieldVectors();
    ColumnVector[] columnVectors = new ColumnVector[fieldVectors.size()];
    Iterator<FieldVector> iterator = fieldVectors.iterator();
    int rowCount = -1;
    for (int i = 0; i < columnVectors.length; i++) {
      FieldVector fieldVector = iterator.next();
      columnVectors[i] = new ArrowColumnVector(fieldVector);
      if (rowCount == -1) {
        //All column vectors have same length so we can get rowCount from any column
        rowCount = fieldVector.getValueCount();
      }
    }
    ColumnarBatch columnarBatch = new ColumnarBatch(columnVectors);
    columnarBatch.setNumRows(rowCount);
    return columnarBatch;
  }

  @Override public void close() throws IOException {
    this.reader.close();
  }

}
