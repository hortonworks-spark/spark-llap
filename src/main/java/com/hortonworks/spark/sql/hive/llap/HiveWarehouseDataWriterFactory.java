package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class HiveWarehouseDataWriterFactory implements DataWriterFactory<InternalRow> {

  protected String jobId;
  protected StructType schema;
  protected String path;
  protected Configuration conf;
  protected byte[] confBytes;

  public HiveWarehouseDataWriterFactory() {
    ByteArrayInputStream confByteArrayStream = new ByteArrayInputStream(confBytes);
    conf = new Configuration();

    try(DataInputStream confByteData = new DataInputStream(confByteArrayStream)) {
      conf.readFields(confByteData);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public HiveWarehouseDataWriterFactory(String jobId, StructType schema, String path,
      byte[] confBytes) {
    this.jobId = jobId;
    this.schema = schema;
    this.path = path;
    this.confBytes = confBytes;
  }

  @Override public DataWriter<InternalRow> createDataWriter(int partitionId, int attemptNumber) {
    Path filePath = new Path(this.path, String.format("%s_%s_%s", jobId, partitionId, attemptNumber));
    FileSystem fs = null;
    try {
      fs = filePath.getFileSystem(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return getDataWriter(conf, jobId, schema, partitionId, attemptNumber,
        fs, filePath);
  }

  protected DataWriter<InternalRow> getDataWriter(Configuration conf, String jobId,
      StructType schema, int partitionId, int attemptNumber,
      FileSystem fs, Path filePath) {
    return new HiveWarehouseDataWriter(conf, jobId, schema, partitionId, attemptNumber, fs, filePath);
  }
}

