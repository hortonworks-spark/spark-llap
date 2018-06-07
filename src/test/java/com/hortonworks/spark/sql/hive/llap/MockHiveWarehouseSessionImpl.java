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

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;

// Exposed for Python side access.
public class MockHiveWarehouseSessionImpl extends HiveWarehouseSessionImpl {

    // Exposed for Python side access.
    public static DriverResultSet testFixture() {
        ArrayList<Row> row = new ArrayList<>();
        row.add(new GenericRow(new Object[] {1, "ID 1"}));
        row.add(new GenericRow(new Object[] {2, "ID 2"}));
        StructType schema = (new StructType())
                .add("col1", "int")
                .add("col2", "string");
        return new DriverResultSet(row, schema);
    }

    public MockHiveWarehouseSessionImpl(HiveWarehouseSessionState sessionState) {
        super(sessionState);
        super.getConnector =
                () -> new MockConnection();
        super.executeStmt =
                (conn, database, sql) -> {
                    try {
                        new org.apache.hadoop.hive.ql.parse.ParseDriver().parse(sql);
                        return testFixture();
                    } catch(ParseException pe) {
                        throw new RuntimeException(pe);
                    }
                };
      super.executeUpdate =
        (conn, database, sql) -> {
            try {
                new org.apache.hadoop.hive.ql.parse.ParseDriver().parse(sql);
                return true;
            } catch(ParseException pe) {
                throw new RuntimeException(pe);
            }
        };
        HiveWarehouseSessionImpl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL =
                "com.hortonworks.spark.sql.hive.llap.SimpleMockConnector";
    }

}
