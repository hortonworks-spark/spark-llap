/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import com.hortonworks.spark.sql.hive.llap.util.TriFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import static com.hortonworks.spark.sql.hive.llap.HWConf.*;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.useDatabase;

public class HiveWarehouseSessionImpl implements com.hortonworks.hwc.HiveWarehouseSession {
  static String HIVE_WAREHOUSE_CONNECTOR_INTERNAL = HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR;

  protected HiveWarehouseSessionState sessionState;

  protected Supplier<Connection> getConnector;

  protected TriFunction<Connection, String, String, DriverResultSet> executeStmt;

  protected TriFunction<Connection, String, String, Boolean> executeUpdate;

  public HiveWarehouseSessionImpl(HiveWarehouseSessionState sessionState) {
    this.sessionState = sessionState;
    getConnector = () -> DefaultJDBCWrapper.getConnector(sessionState);
    executeStmt = (conn, database, sql) ->
      DefaultJDBCWrapper.executeStmt(conn, database, sql, MAX_EXEC_RESULTS.getInt(sessionState));
    executeUpdate = (conn, database, sql) ->
      DefaultJDBCWrapper.executeUpdate(conn, database, sql);
    sessionState.session.listenerManager().register(new LlapQueryExecutionListener());
  }

  public Dataset<Row> q(String sql) {
    return executeQuery(sql);
  }

  public Dataset<Row> executeQuery(String sql) {
    DataFrameReader dfr = session().read().format(HIVE_WAREHOUSE_CONNECTOR_INTERNAL).option("query", sql);
    return dfr.load();
  }

  public Dataset<Row> execute(String sql) {
    try (Connection conn = getConnector.get()) {
      DriverResultSet drs = executeStmt.apply(conn, DEFAULT_DB.getString(sessionState), sql);
      return drs.asDataFrame(session());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean executeUpdate(String sql) {
    try (Connection conn = getConnector.get()) {
      return executeUpdate.apply(conn, DEFAULT_DB.getString(sessionState), sql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean executeUpdateInternal(String sql, Connection conn) {
    return executeUpdate.apply(conn, DEFAULT_DB.getString(sessionState), sql);
  }

  public Dataset<Row> executeInternal(String sql, Connection conn) {
    DriverResultSet drs = executeStmt.apply(conn, DEFAULT_DB.getString(sessionState), sql);
    return drs.asDataFrame(session());
  }

  public Dataset<Row> table(String sql) {
    DataFrameReader dfr = session().read().format(HIVE_WAREHOUSE_CONNECTOR_INTERNAL).option("table", sql);
    return dfr.load();
  }

  public SparkSession session() {
    return sessionState.session;
  }

  // Exposed for Python side.
  public HiveWarehouseSessionState sessionState() {
    return sessionState;
  }

  SparkConf conf() {
    return sessionState.session.sparkContext().getConf();
  }

  /* Catalog helpers */
  public void setDatabase(String name) {
    HWConf.DEFAULT_DB.setString(sessionState, name);
  }

  public Dataset<Row> showDatabases() {
    return execute(HiveQlUtil.showDatabases());
  }

  public Dataset<Row> showTables(){
    return execute(HiveQlUtil.showTables(DEFAULT_DB.getString(sessionState)));
  }

  public Dataset<Row> describeTable(String table) {
    return execute(HiveQlUtil.describeTable(DEFAULT_DB.getString(sessionState), table));
  }

  public void dropDatabase(String database, boolean ifExists, boolean cascade) {
    executeUpdate(HiveQlUtil.dropDatabase(database, ifExists, cascade));
  }

  public void dropTable(String table, boolean ifExists, boolean purge) {
    try (Connection conn = getConnector.get()) {
      executeUpdateInternal(HiveQlUtil.useDatabase(DEFAULT_DB.getString(sessionState)), conn);
      String dropTable = HiveQlUtil.dropTable(table, ifExists, purge);
      executeUpdateInternal(dropTable, conn);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void createDatabase(String database, boolean ifNotExists) {
    executeUpdate(HiveQlUtil.createDatabase(database, ifNotExists));
  }

  public CreateTableBuilder createTable(String tableName) {
    return new CreateTableBuilder(this, DEFAULT_DB.getString(sessionState), tableName);
  }


}

