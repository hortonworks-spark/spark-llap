package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    private Writer out;

    public HiveWarehouseDataWriter(Configuration conf, String jobId, StructType schema, SaveMode saveMode, int partitionId, int attemptNumber, FileSystem fs, Path filePath) {
        this.jobId = jobId;
        this.schema = schema;
        this.saveMode = saveMode;
        this.partitionId = partitionId;
        this.attemptNumber = attemptNumber;
        try {
            this.out = SequenceFile.createWriter(conf, SequenceFile.Writer.file(filePath), SequenceFile.Writer.keyClass(BytesWritable.class), SequenceFile.Writer.valueClass(Text.class));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(InternalRow record) throws IOException {
        Seq scalaSeq = record.toSeq(schema);
        List<String> colStrs = new ArrayList<>();
        for(Object colVal : scala.collection.JavaConversions.seqAsJavaList(scalaSeq)) {
           String colStr = (colVal != null) ? colVal.toString() : "null";
           colStrs.add(colStr);
        }
        String row = String.join(",", colStrs);
        out.append(EMPTY_KEY, new Text((row.getBytes())));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        out.close();
        return new SimpleWriterCommitMessage(String.format("COMMIT %s_%s_%s", jobId, partitionId, attemptNumber));
    }

    @Override
    public void abort() throws IOException {
        LOG.info("Driver sent abort for %s_%s_%s", jobId, partitionId, attemptNumber);
        try {
            out.close();
        } finally {
            fs.delete(filePath, false);
        }
    }
}
