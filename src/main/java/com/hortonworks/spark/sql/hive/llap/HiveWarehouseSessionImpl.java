package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.api.HiveWarehouseSession;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import com.hortonworks.spark.sql.hive.llap.util.TriFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Supplier;

import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.useDatabase;

public class HiveWarehouseSessionImpl implements HiveWarehouseSession {
    public static String HIVE_WAREHOUSE_CONNECTOR_INTERNAL = HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR;

    public HiveWarehouseSessionState sessionState;

    protected Supplier<Connection> getConnector;

    protected TriFunction<Connection, String, String, DriverResultSet> executeStmt;

    HiveWarehouseSessionImpl(HiveWarehouseSessionState sessionState) {
        this.sessionState = sessionState;
        getConnector = () -> DefaultJDBCWrapper.getConnector(sessionState);
        executeStmt = (conn, database, sql) -> DefaultJDBCWrapper.executeStmt(conn, database, sql);
        sessionState.session().listenerManager().register(new LlapQueryExecutionListener());
    }

    public Dataset<Row> q(String sql) {
      return executeQuery(sql);
    }

    public Dataset<Row> executeQuery(String sql) {
        DataFrameReader dfr = session().read().format(HIVE_WAREHOUSE_CONNECTOR_INTERNAL).option("query", sql);
	dfr = dfr.option("currentdatabase", sessionState.database());
	dfr = dfr.option("user.name", sessionState.user());
	dfr = dfr.option("user.password", sessionState.password());
	dfr = dfr.option("dbcp2.conf", sessionState.dbcp2Conf());
	dfr = dfr.option("url", sessionState.hs2url());
    	return dfr.load();
    }

    public Dataset<Row> exec(String sql) {
      return execute(sql);
    }

    public Dataset<Row> execute(String sql) {
        try(Connection conn = getConnector.get()) {
            DriverResultSet drs = executeStmt.apply(conn, sessionState.database(), sql);
            return session().createDataFrame((List<Row>) drs.data, drs.schema);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Dataset<Row> executeInternal(String sql, Connection conn) {
        DriverResultSet drs = executeStmt.apply(conn, sessionState.database(), sql);
        return session().createDataFrame((List<Row>) drs.data, drs.schema);
    }

    public Dataset<Row> table(String sql) {
        return session().read().format(HIVE_WAREHOUSE_CONNECTOR_INTERNAL).option("table", sql).load();
    }

    public SparkSession session() {
        return sessionState.session();
    }

    SparkConf conf() {
        return sessionState.session().sparkContext().getConf();
    }

    /* Catalog helpers */
    public void setDatabase(String name) {
        this.sessionState.defaultDB = name;
    }

    @Override
    public Dataset<Row> showDatabases() {
        return exec(HiveQlUtil.showDatabases());
    }

    public Dataset<Row> showTables() {
        return exec(HiveQlUtil.showTables(this.sessionState.database()));
    }

    public Dataset<Row> describeTable(String table) {
        return exec(HiveQlUtil.describeTable(this.sessionState.database(), table));
    }

    public void dropDatabase(String database, boolean ifExists, boolean cascade) {
        exec(HiveQlUtil.dropDatabase(database, ifExists, cascade));
    }

    public void dropTable(String table, boolean ifExists, boolean purge) {
        try(Connection conn = getConnector.get()) {
            executeInternal(HiveQlUtil.useDatabase(this.sessionState.database()), conn);
            executeInternal(HiveQlUtil.dropTable(table, ifExists, purge), conn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createDatabase(String database, boolean ifNotExists) {
        exec(HiveQlUtil.createDatabase(database, ifNotExists));
    }

    @Override
    public CreateTableBuilder createTable(String tableName) {
        return new CreateTableBuilder(this, sessionState.database(), tableName);
    }

}

