package com.hortonworks.spark.sql.hive.llap;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.SessionConfigSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveWarehouseDataSource implements DataSourceV2, ReadSupport, SessionConfigSupport {

    private static final String HIVE_WAREHOUSE_PREFIX = "hive.warehouse";
    private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataSource.class);

        @Override
        public DataSourceReader createReader(DataSourceOptions options) {
            try {
                SparkSession session = SparkSession.getActiveSession().get();
                if(session == null) {
                    throw new IllegalStateException("Must have active SparkSession to use HiveWarehouse");
                }
                Map<String, String> params = new HashMap<>();
                String connectionUrl = options.get("url").get();
                params.put("query", options.get("query").get());
                params.put("table", options.get("table").get());
                //TODO username/password
                params.put("user.name", "");
                params.put("user.password", "");
                //TODO handle dbcp.conf
                params.put("dbcp2.conf", null);
                params.put("url", connectionUrl);
                return new HiveWarehouseDataSourceReader(params);
            } catch (IOException e) {
                LOG.error("Error creating {}", getClass().getName());
                LOG.error(ExceptionUtils.getStackTrace(e));
            }
            return null;
        }

    @Override
    public String keyPrefix() {
        return HIVE_WAREHOUSE_PREFIX;
    }
}
