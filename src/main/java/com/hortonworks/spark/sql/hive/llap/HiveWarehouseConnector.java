package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.api.HiveWarehouseSession;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.SessionConfigSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveWarehouseConnector implements DataSourceV2, ReadSupport, SessionConfigSupport, WriteSupport {

    private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseConnector.class);

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
		} else {
                	params.put("user.name", "hive");
		}
		if(options.get("user.password").isPresent()) {
                	params.put("user.password", options.get("user.password").get());
		} else {
                	params.put("user.password", "hive");
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
        return HiveWarehouseSession.HIVE_WAREHOUSE_CONF;
    }

    @Override
    public Optional<DataSourceWriter> createWriter(String jobId, StructType schema, SaveMode mode, DataSourceOptions options) {
        Path path = new Path(options.get("path").get());
	Configuration conf = SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
        return Optional.of(new HiveWarehouseDataSourceWriter(jobId, schema, mode, path, conf));
    }
}
