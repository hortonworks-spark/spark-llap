package com.hortonworks.spark.sql.hive.llap;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.SessionConfigSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/*
 * Driver:
 *   UserCode -> HiveWarehouseConnector -> HiveWarehouseDataSourceReader -> HiveWarehouseDataReaderFactory
 * Task serializer:
 *   HiveWarehouseDataReaderFactory (Driver) -> bytes -> HiveWarehouseDataReaderFactory (Executor task)
 * Executor:
 *   HiveWarehouseDataReaderFactory -> HiveWarehouseDataReader
 */
public class HiveWarehouseConnector implements DataSourceV2, ReadSupport, SessionConfigSupport, WriteSupport {

  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseConnector.class);

  @Override public DataSourceReader createReader(DataSourceOptions options) {
    try {
      return getDataSourceReader(getOptions(options));
    } catch (IOException e) {
      LOG.error("Error creating {}", getClass().getName());
      LOG.error(ExceptionUtils.getStackTrace(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String jobId, StructType schema,
      SaveMode mode, DataSourceOptions options) {
    Map<String, String> params = getOptions(options);
    String stagingDirPrefix = HWConf.LOAD_STAGING_DIR.getFromOptionsMap(params);
    Path path = new Path(stagingDirPrefix);
    Configuration conf = SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
    return Optional.of(getDataSourceWriter(jobId, schema, path, params, conf));
  }

  @Override public String keyPrefix() {
    return HiveWarehouseSession.HIVE_WAREHOUSE_POSTFIX;
  }

  private static Map<String, String> getOptions(DataSourceOptions options) {
    return options.asMap();
  }

  protected DataSourceReader getDataSourceReader(Map<String, String> params) throws IOException {
    return new HiveWarehouseDataSourceReader(params);
  }

  protected DataSourceWriter getDataSourceWriter(String jobId, StructType schema,
      Path path, Map<String, String> options, Configuration conf) {
    return new HiveWarehouseDataSourceWriter(options, jobId, schema, path, conf);
  }

}
