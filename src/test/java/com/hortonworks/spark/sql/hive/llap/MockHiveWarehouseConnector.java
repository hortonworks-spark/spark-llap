package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.api.HiveWarehouseSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.SessionConfigSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

public class MockHiveWarehouseConnector implements DataSourceV2, ReadSupport, SessionConfigSupport {

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return new MockHiveWarehouseDataSourceReader();
    }

    @Override
    public String keyPrefix() {
        return HiveWarehouseSession.HIVE_WAREHOUSE_CONF;
    }


}
