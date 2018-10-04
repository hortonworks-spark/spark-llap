package com.hortonworks.spark.sql.hive.llap.streaming;

import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.spark.sql.hive.llap.HiveStreamingDataWriterFactory;

public class HiveStreamingDataSourceWriter implements SupportsWriteInternalRow, StreamWriter {
  private static Logger LOG = LoggerFactory.getLogger(HiveStreamingDataSourceWriter.class);

  private String jobId;
  private StructType schema;
  private String db;
  private String table;
  private List<String> partition;
  private String metastoreUri;
  private String metastoreKerberosPrincipal;

  public HiveStreamingDataSourceWriter(String jobId, StructType schema, String db,
    String table, List<String> partition, final String metastoreUri, final String metastoreKerberosPrincipal) {
    this.jobId = jobId;
    this.schema = schema;
    this.db = db;
    this.table = table;
    this.partition = partition;
    this.metastoreUri = metastoreUri;
    this.metastoreKerberosPrincipal = metastoreKerberosPrincipal;
  }

  @Override
  public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
    // for the streaming case, commit transaction happens on task commit() (atleast-once), so interval is set to -1
    return new HiveStreamingDataWriterFactory(jobId, schema, -1, db, table, partition, metastoreUri,
      metastoreKerberosPrincipal);
  }

  @Override
  public void commit(final long epochId, final WriterCommitMessage[] messages) {
    LOG.info("Commit job {}", jobId);
  }

  @Override
  public void abort(final long epochId, final WriterCommitMessage[] messages) {
    LOG.info("Abort job {}", jobId);
  }
}

