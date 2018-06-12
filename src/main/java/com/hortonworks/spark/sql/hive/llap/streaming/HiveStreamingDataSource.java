package com.hortonworks.spark.sql.hive.llap.streaming;

import java.util.Arrays;
import java.util.List;

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.StreamWriteSupport;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.sources.v2.SessionConfigSupport;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveStreamingDataSource implements DataSourceV2, StreamWriteSupport, SessionConfigSupport {
  private static final long DEFAULT_COMMIT_INTERVAL_ROWS = 10000;
  private static Logger LOG = LoggerFactory.getLogger(HiveStreamingDataSource.class);

  @Override
  public StreamWriter createStreamWriter(final String queryId, final StructType schema, final OutputMode mode,
    final DataSourceOptions options) {
    return createDataSourceWriter(queryId, schema, options);
  }

  private HiveStreamingDataSourceWriter createDataSourceWriter(final String id, final StructType schema,
    final DataSourceOptions options) {
    String dbName = null;
    if(options.get("default.db").isPresent()) {
      dbName = options.get("default.db").get();
    } else {
      dbName = options.get("database").orElse("default");
    }
    String tableName = options.get("table").orElse(null);
    String partition = options.get("partition").orElse(null);
    List<String> partitionValues = partition == null ? null : Arrays.asList(partition.split(","));
    String metastoreUri = options.get("metastoreUri").orElse("thrift://localhost:9083");
    String commitIntervalRows = options.get("commitIntervalRows").orElse("" + DEFAULT_COMMIT_INTERVAL_ROWS);
    long commitInterval = Long.parseLong(commitIntervalRows);
    LOG.info("OPTIONS - database: {} table: {} partition: {} commitIntervalRows: {} metastoreUri: {}", dbName,
      tableName, partition, commitInterval, metastoreUri);
    return new HiveStreamingDataSourceWriter(id, schema, commitInterval, dbName, tableName,
      partitionValues, metastoreUri);
  }

  @Override public String keyPrefix() {
    return HiveWarehouseSession.HIVE_WAREHOUSE_POSTFIX;
  }

}

