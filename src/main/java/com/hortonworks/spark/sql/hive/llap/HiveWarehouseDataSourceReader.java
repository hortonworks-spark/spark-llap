package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import static com.hortonworks.spark.sql.hive.llap.FilterPushdown.buildWhereClause;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.*;
import static com.hortonworks.spark.sql.hive.llap.util.JobUtil.replaceSparkHiveDriver;
import static scala.collection.JavaConversions.asScalaBuffer;

/**
 * 1. Spark pulls the unpruned schema -> readSchema()
 * 2. Spark pushes the pruned schema -> pruneColumns(..)
 * 3. Spark pushes the top-level filters -> pushFilters(..)
 * 4. Spark pulls the filters that are supported by datasource -> pushedFilters(..)
 * 5. Spark pulls factories, where factory/task are 1:1 -> createBatchDataReaderFactories(..)
 */
public class HiveWarehouseDataSourceReader
    implements DataSourceReader, SupportsPushDownRequiredColumns, SupportsScanColumnarBatch, SupportsPushDownFilters {

  //The pruned schema
  StructType schema = null;

  //The original schema
  StructType baseSchema = null;

  //Pushed down filters
  //
  //"It's possible that there is no filters in the query and pushFilters(Filter[])
  // is never called, empty array should be returned for this case."
  Filter[] pushedFilters = new Filter[0];

  //SessionConfigSupport options
  Map<String, String> options;

  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataSourceReader.class);

  public HiveWarehouseDataSourceReader(Map<String, String> options) throws IOException {
    this.options = options;
  }

  //if(schema is empty) -> df.count()
  //else if(using table option) -> select *
  //else -> SELECT <COLUMNS> FROM (<RAW_SQL>) WHERE <FILTER_CLAUSE>
  String getQueryString(String[] requiredColumns, Filter[] filters) throws Exception {
    String selectCols = "count(*)";
    if (requiredColumns.length > 0) {
      selectCols = projections(requiredColumns);
    }
    String baseQuery;
    if (getQueryType().equals("table")) {
      baseQuery = selectStar(options.get("table"));
    } else {
      baseQuery = options.get("query");
    }

    Seq<Filter> filterSeq = asScalaBuffer(Arrays.asList(filters)).seq();
    String whereClause = buildWhereClause(baseSchema, filterSeq);
    return selectProjectAliasFilter(selectCols, baseQuery, randomAlias(), whereClause);
  }

  private StatementType getQueryType() throws Exception {
    return StatementType.fromOptions(options);
  }

  protected StructType getTableSchema() throws Exception {
    replaceSparkHiveDriver();

    StatementType queryKey = getQueryType();
      String query;
      if (queryKey == StatementType.FULL_TABLE_SCAN) {
        SchemaUtil.TableRef tableRef = SchemaUtil.getDbTableNames(options.get("table"));
        query = selectStar(tableRef.databaseName, tableRef.tableName);
      } else {
        query = options.get("query");
      }
      LlapBaseInputFormat llapInputFormat = null;
      try {
        JobConf conf = JobUtil.createJobConf(options, query);
        llapInputFormat = new LlapBaseInputFormat(false, Long.MAX_VALUE);
        InputSplit[] splits = llapInputFormat.getSplits(conf, 0);
        LlapInputSplit schemaSplit = (LlapInputSplit) splits[0];
        Schema schema = schemaSplit.getSchema();
        return SchemaUtil.convertSchema(schema);
      } finally {
        if(llapInputFormat != null) {
          close();
        }
      }
  }

  @Override public StructType readSchema() {
    try {
      if (schema == null) {
        this.schema = getTableSchema();
        this.baseSchema = this.schema;
      }
      return schema;
    } catch (Exception e) {
      LOG.error("Unable to read table schema");
      throw new RuntimeException(e);
    }
  }

  //"returns unsupported filters."
  @Override public Filter[] pushFilters(Filter[] filters) {
    pushedFilters = Arrays.stream(filters).
        filter((filter) -> FilterPushdown.buildFilterExpression(baseSchema, filter).isDefined()).
        toArray(Filter[]::new);

    return Arrays.stream(filters).
        filter((filter) -> !FilterPushdown.buildFilterExpression(baseSchema, filter).isDefined()).
        toArray(Filter[]::new);
  }

  @Override public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override public void pruneColumns(StructType requiredSchema) {
    this.schema = requiredSchema;
  }

  @Override public List<DataReaderFactory<ColumnarBatch>> createBatchDataReaderFactories() {
    try {
      boolean countStar = this.schema.length() == 0;
      String queryString = getQueryString(SchemaUtil.columnNames(schema), pushedFilters);
      List<DataReaderFactory<ColumnarBatch>> factories = new ArrayList<>();
      if (countStar) {
        LOG.info("Executing count with query: {}", queryString);
        factories.addAll(getCountStarFactories(queryString));
      } else {
        factories.addAll(getSplitsFactories(queryString));
      }
      return factories;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected List<DataReaderFactory<ColumnarBatch>> getSplitsFactories(String query) {
    List<DataReaderFactory<ColumnarBatch>> tasks = new ArrayList<>();
    try {
      JobConf jobConf = JobUtil.createJobConf(options, query);
      LlapBaseInputFormat llapInputFormat = new LlapBaseInputFormat(false, Long.MAX_VALUE);
      //numSplits arg not currently supported, use 1 as dummy arg
      InputSplit[] splits = llapInputFormat.getSplits(jobConf, 1);
      for (InputSplit split : splits) {
        tasks.add(getDataReaderFactory(split, jobConf, getArrowAllocatorMax()));
      }
    } catch (IOException e) {
      LOG.error("Unable to submit query to HS2");
      throw new RuntimeException(e);
    }
    return tasks;
  }

  protected DataReaderFactory<ColumnarBatch> getDataReaderFactory(InputSplit split, JobConf jobConf, long arrowAllocatorMax) {
    return new HiveWarehouseDataReaderFactory(split, jobConf, arrowAllocatorMax);
  }

  private List<DataReaderFactory<ColumnarBatch>> getCountStarFactories(String query) {
    List<DataReaderFactory<ColumnarBatch>> tasks = new ArrayList<>(100);
    long count = getCount(query);
    String numTasksString = HWConf.COUNT_TASKS.getFromOptionsMap(options);
    int numTasks = Integer.parseInt(numTasksString);
    long numPerTask = count/(numTasks - 1);
    long numLastTask = count % (numTasks - 1);
    for(int i = 0; i < (numTasks - 1); i++) {
      tasks.add(new CountDataReaderFactory(numPerTask));
    }
    tasks.add(new CountDataReaderFactory(numLastTask));
    return tasks;
  }

  protected long getCount(String query) {
    try(Connection conn = getConnection()) {
      DriverResultSet rs = DefaultJDBCWrapper.executeStmt(conn, HWConf.DEFAULT_DB.getFromOptionsMap(options), query,
          Long.parseLong(HWConf.MAX_EXEC_RESULTS.getFromOptionsMap(options)));
      return rs.getData().get(0).getLong(0);
    } catch (SQLException e) {
      LOG.error("Failed to connect to HS2", e);
      throw new RuntimeException(e);
    }
  }

  private Connection getConnection() {
    String url = HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options);
    String user = HWConf.USER.getFromOptionsMap(options);
    String dbcp2Configs = HWConf.DBCP2_CONF.getFromOptionsMap(options);
    return DefaultJDBCWrapper.getConnector(Option.empty(), url, user, dbcp2Configs);
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
