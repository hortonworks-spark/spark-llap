package com.hortonworks.spark.sql.hive.llap;

import static java.lang.String.*;
import scala.Option;
import java.sql.Connection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class HiveWarehouseDataSourceWriter implements SupportsWriteInternalRow {
    private String jobId;
    private StructType schema;
    private SaveMode saveMode;
    private Path path;
    private Configuration conf;
    private Map<String, String> options;
    private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataSourceWriter.class);

    public HiveWarehouseDataSourceWriter(Map<String, String> options, String jobId, StructType schema, SaveMode mode, Path path, Configuration conf) {
        this.options = options;
        this.jobId = jobId;
        this.schema = schema;
        this.saveMode = mode;
        this.path = new Path(new Path(path, "%s_temporary"), jobId);
        this.conf = conf;
    }

    @Override
    public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
        return new HiveWarehouseDataWriterFactory(jobId, schema, saveMode, path, new SerializableConfiguration(conf));
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      String url = options.get("url");
      String user = options.get("user.name");
      String dbcp2Configs = options.get("dbcp2.conf");
      String database = options.get("currentdatabase");
      String table = options.get("table");
      try(Connection conn = DefaultJDBCWrapper.getConnector(Option.empty(), url, user, dbcp2Configs)) {
        DefaultJDBCWrapper.execute(conn, database, loadInto(this.path.toString(), database, table));	
      } catch(java.sql.SQLException e) {
        throw new RuntimeException(e);
      }
      LOG.info("Commit job {}", jobId);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        LOG.info("Abort job {}", jobId);
    }

    private String loadInto(String path, String database, String table) {
	return format("LOAD DATA INPATH '%s' INTO TABLE %s.%s", path, database, table);
    }
}
