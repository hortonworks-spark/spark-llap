package com.hortonworks.spark.sql.hive.llap.api;

import com.hortonworks.spark.sql.hive.llap.CreateTableBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface HiveWarehouseSession {

    String HIVE_WAREHOUSE_CONNECTOR = "com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector";
    String HIVE_WAREHOUSE_CONF = "spark.datasources.hive.warehouse";
    Long DEFAULT_EXEC_RESULT_MAX = 10000L;
    String USER_KEY = warehouseKey("user.name");
    String PASSWORD_KEY = warehouseKey("password");
    String HS2_URL_KEY = warehouseKey("hs2.url");
    String EXEC_RESULTS_MAX_KEY = warehouseKey("exec.results.max");
    String DBCP2_CONF_KEY = warehouseKey("dbcp2.conf");
    String DEFAULT_DB_KEY = warehouseKey("default.database");

    static String warehouseKey(String suffix) {
        return HIVE_WAREHOUSE_CONF + "." + suffix;
    }

    Dataset<Row> executeQuery(String sql);
    Dataset<Row> q(String sql);

    Dataset<Row> execute(String sql);
    Dataset<Row> exec(String sql);

    Dataset<Row> table(String sql);

    SparkSession session();

    void setDatabase(String name);

    Dataset<Row> showDatabases();

    Dataset<Row> showTables();

    Dataset<Row> describeTable(String table);

    void createDatabase(String database, boolean ifNotExists);

    CreateTableBuilder createTable(String tableName);

    void dropDatabase(String database, boolean ifExists, boolean cascade);

    void dropTable(String table, boolean ifExists, boolean purge);
}
