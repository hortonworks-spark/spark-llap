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
                Map<String, String> params = new HashMap<>();
                String connectionUrl = options.get("url").get();
                params.put("url", connectionUrl);
		if(options.get("query").isPresent()) {
                	params.put("query", options.get("query").get());
		}
		if(options.get("table").isPresent()) {
                	params.put("table", options.get("table").get());
		}
		if(options.get("stmt").isPresent()) {
                	params.put("stmt", options.get("stmt").get());
		}
		if(options.get("currentdatabase").isPresent()) {
                	params.put("currentdatabase", options.get("currentdatabase").get());
		}
		if(options.get("user.name").isPresent()) {
                	params.put("user.name", options.get("user.name").get());
		}
		if(options.get("user.password").isPresent()) {
                	params.put("user.password", options.get("user.password").get());
		}
                if(options.get("dbcp2.conf").isPresent()) {
                	params.put("dbcp2.conf", options.get("dbcp2.conf").get());
		}
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
