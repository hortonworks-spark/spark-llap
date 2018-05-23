package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hortonworks.spark.sql.hive.llap.FilterPushdown.buildWhereClause;
import static java.lang.String.format;
import static scala.collection.JavaConversions.asScalaBuffer;

public class HiveWarehouseDataSourceReader
    implements DataSourceReader, SupportsPushDownRequiredColumns, SupportsScanColumnarBatch, SupportsPushDownFilters {

  StructType schema = null;
  Filter[] pushedFilters = new Filter[0];
  Map<String, String> options;
  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataSourceReader.class);

  public HiveWarehouseDataSourceReader(Map<String, String> options) throws IOException {
    this.options = options;
  }

  String getQueryString(String[] requiredColumns, Filter[] filters) throws Exception {
    String selectCols = "count(*)";
    if (requiredColumns.length > 0) {
      selectCols = "`" + String.join("` , `", requiredColumns) + "`";
    }
    String baseQuery = null;
    if (getQueryType().equals("table")) {
      baseQuery = "select * from " + options.get("table");
    } else {
      baseQuery = options.get("query");
    }

    String baseQueryAlias = "q_" + UUID.randomUUID().toString().replaceAll("[^A-Za-z0-9 ]", "");
    Seq<Filter> filterSeq = asScalaBuffer(Arrays.asList(filters)).seq();
    String whereClause = buildWhereClause(schema, filterSeq);

    String format = "select %s from (%s) as %s %s";
    String queryString = format(format, selectCols, baseQuery, baseQueryAlias, whereClause);

    return queryString;
  }

  private StatementType getQueryType() throws Exception {
    return StatementType.fromOptions(options);
  }

  private static class TableRef {
    private String databaseName;
    private String tableName;

    TableRef(String databaseName, String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }
  }

  private TableRef getDbTableNames(String nameStr) {
    String[] nameParts = nameStr.split("\\.");
    if (nameParts.length != 2) {
      throw new IllegalArgumentException("Expected " + nameStr + " to be in the form db.table");
    }
    return new TableRef(nameParts[0], nameParts[1]);
  }

  private Connection getConnection() {
    String url = HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options);
    String user = HWConf.USER.getFromOptionsMap(options);
    String dbcp2Configs = HWConf.DBCP2_CONF.getFromOptionsMap(options);
    return DefaultJDBCWrapper.getConnector(Option.empty(), url, user, dbcp2Configs);
  }

  private StructType getTableSchema() throws Exception {

    StatementType queryKey = getQueryType();
    Connection conn = getConnection();
    try {
      if (queryKey == StatementType.FULL_TABLE_SCAN) {
        TableRef tableRef = getDbTableNames(options.get("table"));
        return DefaultJDBCWrapper.resolveTable(conn, tableRef.databaseName, tableRef.tableName);
      } else {
        System.out.println(options.toString());
        String currentDatabase = HWConf.DEFAULT_DB.getFromOptionsMap(options);
        return DefaultJDBCWrapper.resolveQuery(conn, currentDatabase, options.get("query"));
      }
    } finally {
      conn.close();
    }
  }

  @Override public StructType readSchema() {
    try {
      if (schema == null) {
        schema = getTableSchema();
      }
      return schema;
    } catch (Exception e) {
      LOG.error("Unable to read table schema");
      throw new RuntimeException(e);
    }
  }

  @Override public Filter[] pushFilters(Filter[] filters) {
    pushedFilters = filters;
    return new Filter[0];
  }

  @Override public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override public void pruneColumns(StructType requiredSchema) {
    this.schema = requiredSchema;
  }

  private String[] requiredColumns(StructType schema) {
    String[] requiredColumns = new String[schema.length()];
    int i = 0;
    for (StructField field : schema.fields()) {
      requiredColumns[i] = field.name();
      i++;
    }
    return requiredColumns;
  }

  static JobConf createJobConf(Map<String, String> options, String queryString) {
    JobConf jobConf = new JobConf(SparkContext.getOrCreate().hadoopConfiguration());
    jobConf.set("hive.llap.zk.registry.user", "hive");
    jobConf.set("llap.if.hs2.connection", HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options));
    if (queryString != null) {
      jobConf.set("llap.if.query", queryString);
    }
    jobConf.set("llap.if.user", HWConf.USER.getFromOptionsMap(options));
    jobConf.set("llap.if.pwd", HWConf.PASSWORD.getFromOptionsMap(options));
    if (options.containsKey("default.db")) {
      jobConf.set("llap.if.database", HWConf.DEFAULT_DB.getFromOptionsMap(options));
    }
    if (!options.containsKey("handleid")) {
      String handleId = UUID.randomUUID().toString();
      options.put("handleid", handleId);
    }
    jobConf.set("llap.if.handleid", options.get("handleid"));
    return jobConf;
  }

  @Override public List<DataReaderFactory<ColumnarBatch>> createBatchDataReaderFactories() {
    try {
      boolean countStar = this.schema.length() == 0;
      String queryString = getQueryString(requiredColumns(schema), pushedFilters);
      InputSplit[] splits = null;
      JobConf jobConf = createJobConf(options, queryString);
      List<DataReaderFactory<ColumnarBatch>> factories = new ArrayList<>();
      if (countStar) {
        factories.addAll(handleCountStar(queryString));
      } else {
        LlapBaseInputFormat llapInputFormat = new LlapBaseInputFormat(false, Long.MAX_VALUE);
        try {
          //numSplits doesn't do anything, use 1 as dummy arg
          splits = llapInputFormat.getSplits(jobConf, 1);
          for (InputSplit split : splits) {
            factories.add(new HiveWarehouseDataReaderFactory(split, jobConf, getArrowAllocatorMax()));
          }
        } catch (IOException e) {
          LOG.error("Unable to submit query to HS2");
          throw new RuntimeException(e);
        }
      }
      return factories;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<DataReaderFactory<ColumnarBatch>> handleCountStar(String query) {
    List<DataReaderFactory<ColumnarBatch>> tasks = new ArrayList<>(100);
    Connection conn = getConnection();
    DriverResultSet rs = DefaultJDBCWrapper.executeStmt(conn,
        HWConf.DEFAULT_DB.getFromOptionsMap(options),
        query,
        Long.parseLong(HWConf.MAX_EXEC_RESULTS.getFromOptionsMap(options))
    );
    long count = rs.getData().get(0).getLong(0);
    //long numPerTask = count/100;
    //long numLastTask = count % 100;
    //for(int i = 0; i < 99; i++) {
      tasks.add(new CountDataReaderFactory(numPerTask));
    //}
    //tasks.add(new CountDataReaderFactory(numLastTask));
    return tasks;
  }

  private long getArrowAllocatorMax () {
    String arrowAllocatorMaxString = HWConf.ARROW_ALLOCATOR_MAX.getFromOptionsMap(options);
    long arrowAllocatorMax = (Long) HWConf.ARROW_ALLOCATOR_MAX.defaultValue;
    if (arrowAllocatorMaxString != null) {
      arrowAllocatorMax = Long.parseLong(arrowAllocatorMaxString);
    }
    LOG.debug("Ceiling for Arrow direct buffers {}", arrowAllocatorMax);
    return arrowAllocatorMax;
  }

  public void close() {
    LOG.info("Closing resources for handleid: {}", options.get("handleid"));
    try {
      LlapBaseInputFormat.close(options.get("handleid"));
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

}
