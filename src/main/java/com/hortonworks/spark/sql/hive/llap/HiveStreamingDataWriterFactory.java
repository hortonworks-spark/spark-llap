package com.hortonworks.spark.sql.hive.llap;

import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

import com.hortonworks.spark.hive.utils.HiveIsolatedClassLoader;

public class HiveStreamingDataWriterFactory implements DataWriterFactory<InternalRow> {

  private String jobId;
  private StructType schema;
  private long commitIntervalRows;
  private String db;
  private String table;
  private List<String> partition;
  private String metastoreUri;
  private String metastoreKrbPrincipal;

  public HiveStreamingDataWriterFactory(String jobId, StructType schema, long commitIntervalRows, String db,
    String table, List<String> partition, final String metastoreUri, final String metastoreKrbPrincipal) {
    this.jobId = jobId;
    this.schema = schema;
    this.db = db;
    this.table = table;
    this.partition = partition;
    this.commitIntervalRows = commitIntervalRows;
    this.metastoreUri = metastoreUri;
    this.metastoreKrbPrincipal = metastoreKrbPrincipal;
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, int attemptNumber) {
    ClassLoader restoredClassloader = Thread.currentThread().getContextClassLoader();
    ClassLoader isolatedClassloader = HiveIsolatedClassLoader.isolatedClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(isolatedClassloader);
      return new HiveStreamingDataWriter(jobId, schema, commitIntervalRows, partitionId, attemptNumber, db,
        table, partition, metastoreUri, metastoreKrbPrincipal);
    } finally {
      Thread.currentThread().setContextClassLoader(restoredClassloader);
    }
  }
}

