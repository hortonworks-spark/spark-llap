package com.hortonworks.spark.sql.hive.llap.api;

import com.hortonworks.spark.sql.hive.llap.CreateTableBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface HiveWarehouseSession {

    String HIVE_WAREHOUSE_CONNECTOR = "com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector";

    Dataset<Row> executeQuery(String sql);
    Dataset<Row> q(String sql);

    Dataset<Row> execute(String sql);

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
