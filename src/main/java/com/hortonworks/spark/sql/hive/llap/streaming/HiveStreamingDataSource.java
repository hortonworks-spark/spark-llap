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
    String metastoreKerberosPrincipal = options.get("metastoreKrbPrincipal").orElse(null);
    LOG.info("OPTIONS - database: {} table: {} partition: {} metastoreUri: {} metastoreKerberosPrincipal: {}",
      dbName, tableName, partition, metastoreUri, metastoreKerberosPrincipal);
    return new HiveStreamingDataSourceWriter(id, schema, dbName, tableName,
      partitionValues, metastoreUri, metastoreKerberosPrincipal);
  }

  @Override public String keyPrefix() {
    return HiveWarehouseSession.HIVE_WAREHOUSE_POSTFIX;
  }

}

