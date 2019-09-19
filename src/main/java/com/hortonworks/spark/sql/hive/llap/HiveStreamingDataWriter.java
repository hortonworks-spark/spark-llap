package com.hortonworks.spark.sql.hive.llap;

import java.io.ByteArrayInputStream;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.streaming.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JacksonGenerator;
import org.apache.spark.sql.catalyst.json.JacksonGeneratorHelper;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class HiveStreamingDataWriter implements DataWriter<InternalRow> {
  private static Logger LOG = LoggerFactory.getLogger(HiveStreamingDataWriter.class);

  private final Charset UTF8_CHARSET = Charset.forName("UTF-8");

  private String jobId;
  private StructType schema;
  private int partitionId;
  private int attemptNumber;
  private WriterType writerType;
  private String db;
  private String table;
  private List<String> partition;
  private String metastoreUri;
  private StreamingConnection streamingConnection;
  private long commitAfterNRows;
  private long rowsWritten = 0;
  private String metastoreKrbPrincipal;
  private JacksonGenerator jacksonGenerator;
  private CharArrayWriter charArrayWriter;


  public HiveStreamingDataWriter(String jobId, StructType schema, long commitAfterNRows, int partitionId, int
    attemptNumber, WriterType writerType, String db, String table, List<String> partition, final String metastoreUri,
    final String metastoreKrbPrincipal) {
    this.jobId = jobId;
    this.schema = schema;
    this.partitionId = partitionId;
    this.attemptNumber = attemptNumber;
    this.writerType = writerType;
    this.db = db;
    this.table = table;
    this.partition = partition;
    this.metastoreUri = metastoreUri;
    this.commitAfterNRows = commitAfterNRows;
    this.metastoreKrbPrincipal = metastoreKrbPrincipal;
    try {
      createStreamingConnection();
    } catch (StreamingException e) {
      throw new RuntimeException("Unable to create hive streaming connection", e);
    }
  }

  private void createStreamingConnection() throws StreamingException {
    AbstractRecordWriter inputWriter;
    if (writerType == WriterType.JSON) {
      inputWriter = StrictJsonWriter.newBuilder().build();
    } else {
      inputWriter = StrictDelimitedInputWriter.newBuilder()
              .withFieldDelimiter(',').build();
    }
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metastoreUri);
    // isolated classloader and shadeprefix are required for reflective instantiation of outputformat class when
    // creating hive streaming connection (isolated classloader to load different hive versions and shadeprefix for
    // HIVE-19494)
    hiveConf.setVar(HiveConf.ConfVars.HIVE_CLASSLOADER_SHADE_PREFIX, "shadehive");
    hiveConf.set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getHiveName(), "hive");

    // HiveStreamingCredentialProvider requires metastore uri and kerberos principal to obtain delegation token to
    // talk to secure metastore. When HiveConf is created above, hive-site.xml is used from classpath. This option
    // is just an override in case if kerberos principal cannot be obtained from hive-site.xml.
    if (metastoreKrbPrincipal != null) {
      hiveConf.set(MetastoreConf.ConfVars.KERBEROS_PRINCIPAL.getHiveName(), metastoreKrbPrincipal);
    }

    if (writerType == WriterType.JSON) {
      charArrayWriter = new CharArrayWriter();
      jacksonGenerator = JacksonGeneratorHelper.createJacksonGenerator(schema, charArrayWriter, null);
    }

    LOG.info("Creating hive streaming connection..");
    streamingConnection = HiveStreamingConnection.newBuilder()
      .withDatabase(db)
      .withTable(table)
      .withStaticPartitionValues(partition)
      .withRecordWriter(inputWriter)
      .withHiveConf(hiveConf)
      .withAgentInfo(jobId + "(" + partitionId + ")")
      .connect();
    streamingConnection.beginTransaction();
    LOG.info("{} created hive streaming connection.", streamingConnection.getAgentInfo());
  }

  @Override
  public void write(final InternalRow record) throws IOException {
    ByteArrayInputStream bais;
    if (writerType == WriterType.JSON) {
      jacksonGenerator.write(record);
      jacksonGenerator.flush();
      String jsonRow = charArrayWriter.toString();
      charArrayWriter.reset();
      bais = new ByteArrayInputStream(jsonRow.getBytes(UTF8_CHARSET));
    } else {
      String delimitedRow = Joiner.on(",").useForNull("")
              .join(scala.collection.JavaConversions.seqAsJavaList(record.toSeq(schema)));
      bais = new ByteArrayInputStream(delimitedRow.getBytes(UTF8_CHARSET));
    }
    try {
      streamingConnection.write(bais);
      rowsWritten++;
      if (rowsWritten > 0 && commitAfterNRows > 0 && (rowsWritten % commitAfterNRows == 0)) {
        LOG.info("Committing transaction after rows: {}", rowsWritten);
        streamingConnection.commitTransaction();
        streamingConnection.beginTransaction();
      }
    } catch (StreamingException e) {
      throw new IOException(e);
    }
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    try {
      streamingConnection.commitTransaction();
    } catch (StreamingException e) {
      throw new IOException(e);
    }
    String msg = "Committed jobId: " + jobId + " partitionId: " + partitionId + " attemptNumber: " + attemptNumber +
      " connectionStats: " + streamingConnection.getConnectionStats();
    streamingConnection.close();
    if (writerType == WriterType.JSON) {
      jacksonGenerator.close();
      charArrayWriter.close();
    }
    LOG.info("Closing streaming connection on commit. Msg: {} rowsWritten: {}", rowsWritten);
    return new SimpleWriterCommitMessage(msg);
  }

  @Override
  public void abort() throws IOException {
    if (streamingConnection != null) {
      try {
        streamingConnection.abortTransaction();
      } catch (StreamingException e) {
        throw new IOException(e);
      }
      String msg = "Aborted jobId: " + jobId + " partitionId: " + partitionId + " attemptNumber: " + attemptNumber +
        " connectionStats: " + streamingConnection.getConnectionStats();
      streamingConnection.close();
      if (writerType == WriterType.JSON) {
        jacksonGenerator.close();
        charArrayWriter.close();
      }
      LOG.info("Closing streaming connection on abort. Msg: {} rowsWritten: {}", msg, rowsWritten);
    }
  }
}

