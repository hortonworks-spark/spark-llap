package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveWarehouseDataSourceWriter implements SupportsWriteInternalRow {
    private String jobId;
    private StructType schema;
    private SaveMode saveMode;
    private Path path;
    private Configuration conf;
    private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataSourceWriter.class);

    public HiveWarehouseDataSourceWriter(String jobId, StructType schema, SaveMode mode, Path path, Configuration conf) {
        this.jobId = jobId;
        this.schema = schema;
        this.saveMode = mode;
        this.path = path;
        this.conf = conf;
    }

    @Override
    public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
        return new HiveWarehouseDataWriterFactory(jobId, schema, saveMode, path, new SerializableConfiguration(conf));
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
