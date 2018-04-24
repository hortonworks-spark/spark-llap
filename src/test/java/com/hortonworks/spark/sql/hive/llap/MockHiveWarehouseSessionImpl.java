package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MockHiveWarehouseSessionImpl extends HiveWarehouseSessionImpl {

    private DriverResultSet testFixture() {
        ArrayList<Row> row = new ArrayList<>();
        row.add(new GenericRow(new Object[] {1, "ID 1"}));
        row.add(new GenericRow(new Object[] {2, "ID 2"}));
        StructType schema = (new StructType())
                .add("col1", "int")
                .add("col1", "string");
        return new DriverResultSet(row, schema);
    }

    public MockHiveWarehouseSessionImpl(HiveWarehouseSessionState sessionState) {
        super(sessionState);
        super.getConnector =
                () -> new MockConnection();
        super.executeStmt =
                (conn, database, sql) -> {
                    try {
                        org.apache.hadoop.hive.ql.parse.ParseUtils.parse(sql);
                        return testFixture();
                    } catch(ParseException pe) {
                        throw new RuntimeException(pe);
                    }
                };
        HiveWarehouseSessionImpl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL =
                "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector";
    }

}
