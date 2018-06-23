package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.util.SerializableHadoopConfiguration;
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
  private Path path;
  private SerializableHadoopConfiguration conf;

  public HiveWarehouseDataWriterFactory(String jobId, StructType schema,
      Path path, SerializableHadoopConfiguration conf) {
    this.jobId = jobId;
    this.schema = schema;
    this.path = path;
    this.conf = conf;
  }

  @Override public DataWriter<InternalRow> createDataWriter(int partitionId, int attemptNumber) {
    Path filePath = new Path(this.path, String.format("%s_%s_%s", jobId, partitionId, attemptNumber));
    FileSystem fs = null;
    try {
      fs = filePath.getFileSystem(conf.get());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return getDataWriter(conf.get(), jobId, schema, partitionId, attemptNumber,
        fs, filePath);
  }

  protected DataWriter<InternalRow> getDataWriter(Configuration conf, String jobId,
      StructType schema, int partitionId, int attemptNumber,
      FileSystem fs, Path filePath) {
    return new HiveWarehouseDataWriter(conf, jobId, schema, partitionId, attemptNumber, fs, filePath);
  }
}