package com.hortonworks.spark.sql.hive.llap;

import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveStreamingDataSourceWriter implements SupportsWriteInternalRow {
  private static Logger LOG = LoggerFactory.getLogger(HiveStreamingDataSourceWriter.class);

  private String jobId;
  private StructType schema;
  private String db;
  private String table;
  private List<String> partition;
  private long commitIntervalRows;
  private String metastoreUri;
  private String metastoreKrbPrincipal;

  public HiveStreamingDataSourceWriter(String jobId, StructType schema, long commitIntervalRows, String db,
    String table, List<String> partition, final String metastoreUri, final String metastoreKrbPrincipal) {
    this.jobId = jobId;
    this.schema = schema;
    this.commitIntervalRows = commitIntervalRows;
    this.db = db;
    this.table = table;
    this.partition = partition;
    this.metastoreUri = metastoreUri;
    this.metastoreKrbPrincipal = metastoreKrbPrincipal;
  }

  @Override
  public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
    return new HiveStreamingDataWriterFactory(jobId, schema, commitIntervalRows, db, table, partition, metastoreUri,
      metastoreKrbPrincipal);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    LOG.info("Commit job {}", jobId);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    LOG.info("Abort job {}", jobId);
  }
}

