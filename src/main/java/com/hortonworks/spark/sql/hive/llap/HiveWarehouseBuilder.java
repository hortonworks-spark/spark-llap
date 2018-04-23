package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.SparkSession;

public class HiveWarehouseBuilder {

    HiveWarehouseSessionState sessionState = new HiveWarehouseSessionState();

    //Can only be instantiated through session(SparkSession session)
    private HiveWarehouseBuilder() {
    }

    public static HiveWarehouseBuilder session(SparkSession session) {
        HiveWarehouseBuilder builder = new HiveWarehouseBuilder();
        builder.sessionState.session = session;
        return builder;
    }

    public HiveWarehouseBuilder userPassword(String user, String password) {
        this.sessionState.user = user;
        this.sessionState.password = password;
        return this;
    }

    public HiveWarehouseBuilder hs2url(String hs2url) {
        this.sessionState.hs2url = hs2url;
        return this;
    }

    public HiveWarehouseBuilder maxExecResults(long maxExecResults) {
        this.sessionState.maxExecResults = maxExecResults;
        return this;
    }

    public HiveWarehouseBuilder dbcp2Conf(String dbcp2Conf) {
        this.sessionState.dbcp2Conf = dbcp2Conf;
        return this;
    }

    public HiveWarehouseBuilder defaultDB(String defaultDB) {
        this.sessionState.defaultDB = defaultDB;
        return this;
    }

    //This is the only way for application to obtain a HiveWarehouseSessionImpl
    public HiveWarehouseSessionImpl build() {
        return new HiveWarehouseSessionImpl(this.sessionState);
    }

    //Revealed internally for test only (not public)
    HiveWarehouseSessionState sessionStateForTest() {
        return this.sessionState;
    }
}
