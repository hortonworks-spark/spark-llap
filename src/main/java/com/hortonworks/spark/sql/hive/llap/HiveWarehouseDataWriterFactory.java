package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;

public class HiveWarehouseDataWriterFactory implements DataWriterFactory<InternalRow> {

    private String jobId;
    private StructType schema;
    private SaveMode saveMode;
    private Path path;
    private SerializableConfiguration conf;

    public HiveWarehouseDataWriterFactory(String jobId, StructType schema, SaveMode saveMode, Path path, SerializableConfiguration conf) {
        this.jobId = jobId;
        this.schema = schema;
        this.saveMode = saveMode;
        this.path = path;
        this.conf = conf;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, int attemptNumber) {
        Path jobPath = new Path(new Path(path, "_temporary"), jobId);
        Path filePath = new Path(jobPath, String.format("%s_%s_%s", jobId, partitionId, attemptNumber));
        FileSystem fs = null;
        try {
            fs = filePath.getFileSystem(conf.value());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new HiveWarehouseDataWriter(jobId, schema, saveMode, partitionId, attemptNumber, fs, filePath);
    }
}

