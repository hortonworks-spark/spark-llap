package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.sql.Connection;
import scala.Option;
import java.util.List;

public class HiveWarehouseSession {
    public static String SPARK_LLAP_FORMAT = "com.hortonworks.spark.sql.hive.llap.HiveWarehouseDataSource";

    private SparkSession session;
    private String currentDatabase;

    public HiveWarehouseSession(SparkSession session) {
        this.session = session;
        this.session.listenerManager().register(new LlapQueryExecutionListener());
    }

    public DataFrameReader read() {
        return session.read();
    }

    public void setDatabase(String name) {
	this.currentDatabase = name;
    }

    public Dataset<Row> q(String sql) {
      return executeQuery(sql);
    }

    public Dataset<Row> executeQuery(String sql) {
        DataFrameReader dfr = session.read().format(SPARK_LLAP_FORMAT).option("query", sql);
	if(this.currentDatabase != null) {
		dfr = dfr.option("currentdatabase", this.currentDatabase);
	}
	return dfr.load();
    }

    public Dataset<Row> exec(String sql) {
      return execute(sql);
    }

    public Dataset<Row> execute(String sql) {
        String url = session.sparkContext().getConf().get("spark.datasource.hive.warehouse.url");
        String user = "";
	if(session.sparkContext().getConf().contains("user.name")) {
		user = session.sparkContext().getConf().get("user.name");
        }
        String dbcp2Configs = null;
	if(session.sparkContext().getConf().contains("spark.datasource.hive.warehouse.dbcp2.conf")) {
		dbcp2Configs = session.sparkContext().getConf().get("spark.datasource.hive.warehouse.dbcp2.conf");
	}
        Connection conn = DefaultJDBCWrapper.getConnector(Option.empty(), url, user, dbcp2Configs);
        DriverResultSet drs = DefaultJDBCWrapper.executeStmt(conn, currentDatabase, sql);
        return session.createDataFrame((List<Row>) drs.data, drs.schema);
    }

    public Dataset<Row> table(String sql) {
        return session.read().format(SPARK_LLAP_FORMAT).option("table", sql).load();
    }

    public SparkSession session() {
        return session;
    }

}

