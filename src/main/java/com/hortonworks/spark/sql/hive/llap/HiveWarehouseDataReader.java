package com.hortonworks.spark.sql.hive.llap;

import org.apache.arrow.vector.FieldVector;
import org.apache.hadoop.hive.llap.LlapArrowBatchRecordReader;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.hive.llap.SubmitWorkInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.hive.ql.io.arrow.RootAllocatorFactory;

public class HiveWarehouseDataReader implements InputPartitionReader<ColumnarBatch> {

  private RecordReader<?, ArrowWrapperWritable> reader;
  private ArrowWrapperWritable wrapperWritable = new ArrowWrapperWritable();
  //Reuse single instance of ColumnarBatch and ColumnVector[]
  //See org.apache.spark.sql.vectorized.ColumnarBatch
  //"Instance of it is meant to be reused during the entire data loading process."
  private ColumnarBatch columnarBatch;
  private ColumnVector[] columnVectors;
  private long allocatorMax;
  private BufferAllocator allocator;
  private String attemptId;

  public HiveWarehouseDataReader(LlapInputSplit split, JobConf conf, long arrowAllocatorMax) throws Exception {
    //Set TASK_ATTEMPT_ID to submit to LlapOutputFormatService
    this.allocatorMax = arrowAllocatorMax;
    this.attemptId = getTaskAttemptID(split, conf).toString();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, this.attemptId);
    this.reader = getRecordReader(split, conf, arrowAllocatorMax);
  }

  protected TaskAttemptID getTaskAttemptID(LlapInputSplit split, JobConf conf) throws IOException {
    //Get pseudo-ApplicationId to submit task attempt from external client
    SubmitWorkInfo submitWorkInfo = SubmitWorkInfo.fromBytes(split.getPlanBytes());
    ApplicationId appId = submitWorkInfo.getFakeAppId();
    JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
    //Create TaskAttemptID from Spark TaskContext (TaskType doesn't matter)
    return new TaskAttemptID(new TaskID(jobId, TaskType.MAP, TaskContext.get().partitionId()), TaskContext.get().attemptNumber());
  }

  protected RecordReader<?, ArrowWrapperWritable> getRecordReader(LlapInputSplit split, JobConf conf, long arrowAllocatorMax)
      throws IOException {
    //Use per-task allocator for accounting only, no need to reserve per-task memory
    long childAllocatorReservation = 0L;
    //Break out accounting of direct memory per-task, so we can check no memory is leaked when task is completed
    this.allocator = RootAllocatorFactory.INSTANCE.getOrCreateRootAllocator(arrowAllocatorMax).newChildAllocator(
        attemptId,
        childAllocatorReservation,
        arrowAllocatorMax);
    LlapBaseInputFormat input = new LlapBaseInputFormat(true, arrowAllocatorMax);
    return input.getRecordReader(split, conf, null);
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
    if(columnVectors == null) {
      //Lazy create ColumnarBatch/ColumnVector[] instance
      columnVectors = new ColumnVector[fieldVectors.size()];
      columnarBatch = new ColumnarBatch(columnVectors);
    }
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
    columnarBatch.setNumRows(rowCount);
    return columnarBatch;
  }

  @Override public void close() throws IOException {
    //close() single ColumnarBatch instance
    columnarBatch.close();
    //reader.close() will throw exception unless all arrow buffers have been released
    //See org.apache.hadoop.hive.llap.close()
    //See org.apache.arrow.memory.BaseAllocator.close()
    this.reader.close();
  }

}

