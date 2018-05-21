package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.orc.OrcOutputWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HiveWarehouseDataWriter implements DataWriter<InternalRow> {
  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataWriter.class);
  private BytesWritable EMPTY_KEY = new BytesWritable();

  private String jobId;
  private StructType schema;
  private SaveMode saveMode;
  private int partitionId;
  private int attemptNumber;
  private FileSystem fs;
  private Path filePath;
  private OrcOutputWriter out;

  public HiveWarehouseDataWriter(Configuration conf, String jobId, StructType schema, SaveMode saveMode,
      int partitionId, int attemptNumber, FileSystem fs, Path filePath) {
    this.jobId = jobId;
    this.schema = schema;
    this.saveMode = saveMode;
    this.partitionId = partitionId;
    this.attemptNumber = attemptNumber;
    JobConf jobConf = new JobConf(conf);
    jobConf.set("orc.mapred.output.schema", this.schema.simpleString());
    TaskAttemptContext tac = new TaskAttemptContextImpl(jobConf, new TaskAttemptID());
    this.out = new OrcOutputWriter(filePath.toString(), schema, tac);
  }

  @Override public void write(InternalRow record) throws IOException {
    out.write(record);
  }

  @Override public WriterCommitMessage commit() throws IOException {
    out.close();
    return new SimpleWriterCommitMessage(String.format("COMMIT %s_%s_%s", jobId, partitionId, attemptNumber));
  }

  @Override public void abort() throws IOException {
    LOG.info("Driver sent abort for %s_%s_%s", jobId, partitionId, attemptNumber);
    try {
      out.close();
    } finally {
      fs.delete(filePath, false);
    }
  }
}
