package com.hortonworks.spark.sql.hive.llap;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.SessionConfigSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class HiveWarehouseConnector implements DataSourceV2, ReadSupport, SessionConfigSupport {

  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseConnector.class);

  @Override public DataSourceReader createReader(DataSourceOptions options) {
    try {
      Map<String, String> params = getOptions(options);
      return new HiveWarehouseDataSourceReader(params);
    } catch (IOException e) {
      LOG.error("Error creating {}", getClass().getName());
      LOG.error(ExceptionUtils.getStackTrace(e));
    }
    return null;
  }

  @Override public String keyPrefix() {
    return com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_POSTFIX;
  }

  private static Map<String, String> getOptions(DataSourceOptions options) {
    return options.asMap();
  }

}
