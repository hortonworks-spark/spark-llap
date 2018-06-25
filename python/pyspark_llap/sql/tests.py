#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import os
import glob

from pyspark.sql import SparkSession

from pyspark_llap.sql import HiveWarehouseBuilder
from pyspark_llap.sql.session import CreateTableBuilder, HiveWarehouseSessionImpl

TEST_USER = "userX"
TEST_PASSWORD = "passwordX"
TEST_HS2_URL = "jdbc:hive2://nohost:10084"
TEST_DBCP2_CONF = "defaultQueryTimeout=100"
TEST_EXEC_RESULTS_MAX = 12345
TEST_DEFAULT_DB = "default12345"

root = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../target"))

basepath = glob.glob("%s/scala-*" % root)
assert len(basepath) > 0, "Build the package first. ./target/scala-* directory was not found."
basepath = basepath[0]

jarpath = glob.glob("%s/hive-warehouse-connector-assembly-*" % basepath)
assert len(jarpath) == 1, \
    "Multiple assemply jars were detected in ./target/scala-* directory or not found %s. " \
    "Please clean up and build again." % jarpath
jarpath = jarpath[0]

testjarpath = glob.glob("%s/hive-warehouse-connector*tests.jar" % basepath)
assert len(testjarpath) == 1, \
    "Multiple test:package jars were detected in ./target/scala-* directory or not found %s. " \
    "Please clean up and build again." % testjarpath
testjarpath = testjarpath[0]


class HiveWarehouseBuilderTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[4]") \
            .appName(cls.__name__) \
            .config(
                "spark.driver.extraClassPath",
                "%s:%s" % (jarpath, testjarpath)) \
            .config(
                "spark.executor.extraClassPath",
                "%s:%s" % (jarpath, testjarpath)) \
            .getOrCreate()

        try:
            cls.spark._jvm.com.hortonworks.spark.sql.hive.llap.MockConnection()
        except:
            cls.tearDownClass()
            raise Exception("PySpark LLAP tests are dependent on mock classes defined in test"
                            "codes. These should be compiled together, for example, by "
                            "'sbt test:package'.")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    conf_pairs = {
        u'spark.datasource.hive.warehouse.password': TEST_PASSWORD,
        u'spark.datasource.hive.warehouse.dbcp2.conf': TEST_DBCP2_CONF,
        u'spark.datasource.hive.warehouse.default.db': TEST_DEFAULT_DB,
        u'spark.datasource.hive.warehouse.user.name': TEST_USER,
        u'spark.datasource.hive.warehouse.exec.results.max': str(TEST_EXEC_RESULTS_MAX),
    }

    def test_all_builder_config(self):
        session = self.spark
        jstate = HiveWarehouseBuilder \
            .session(session) \
            .userPassword(TEST_USER, TEST_PASSWORD) \
            .hs2url(TEST_HS2_URL) \
            .dbcp2Conf(TEST_DBCP2_CONF) \
            .maxExecResults(TEST_EXEC_RESULTS_MAX) \
            .defaultDB(TEST_DEFAULT_DB) \
            ._jhwbuilder.sessionStateForTest()

        hive = session._jvm.com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseSessionImpl(jstate)
        self.assertEqual(dict(hive.sessionState().getProps()), HiveWarehouseBuilderTest.conf_pairs)

    def test_all_config(self):
        session = self.spark

        for key, value in HiveWarehouseBuilderTest.conf_pairs.items():
            session.conf.set(key, value)

        for key in HiveWarehouseBuilderTest.conf_pairs.keys():
            # conf().getOption(key).get(), is used in 'MockHiveWarehouseSessionImpl'.
            self.assertEqual(
                str(session._jsparkSession.conf().getOption(key).get()),
                str(HiveWarehouseBuilderTest.conf_pairs[key]))

    def test_session_build(self):
        session = self.spark
        self.assertTrue(HiveWarehouseBuilder.session(session).build() is not None)

    def test_new_entry_point(self):
        import pyspark_llap

        session = self.spark

        HIVESERVER2_JDBC_URL = "spark.sql.hive.hiveserver2.jdbc.url"
        session.conf.set(HIVESERVER2_JDBC_URL, "test")
        hive = pyspark_llap.HiveWarehouseSession.session(session) \
            .userPassword(TEST_USER, TEST_PASSWORD) \
            .dbcp2Conf(TEST_DBCP2_CONF) \
            .maxExecResults(TEST_EXEC_RESULTS_MAX) \
            .defaultDB(TEST_DEFAULT_DB).build()
        self.assertEqual(hive.session(), session)


class HiveWarehouseSessionHiveQlTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[4]") \
            .appName(cls.__name__) \
            .config(
                "spark.driver.extraClassPath",
                "%s:%s" % (jarpath, testjarpath)) \
            .config(
                "spark.executor.extraClassPath",
                "%s:%s" % (jarpath, testjarpath)) \
            .getOrCreate()
        try:
            cls.spark._jvm.com.hortonworks.spark.sql.hive.llap.MockConnection()
        except:
            cls.tearDownClass()
            raise Exception("PySpark LLAP tests are dependent on mock classes defined in test"
                            "codes. These should be compiled together, for example, by "
                            "'sbt test:package'.")

        session = cls.spark
        jstate = HiveWarehouseBuilder \
            .session(session) \
            .userPassword(TEST_USER, TEST_PASSWORD) \
            .hs2url(TEST_HS2_URL) \
            .dbcp2Conf(TEST_DBCP2_CONF) \
            .maxExecResults(TEST_EXEC_RESULTS_MAX) \
            .defaultDB(TEST_DEFAULT_DB) \
            ._jhwbuilder.sessionStateForTest()

        cls.hive = HiveWarehouseSessionImpl(
            session,
            session._jvm.com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseSessionImpl(jstate))
        cls.mockExecuteResultSize = session._jvm.com.hortonworks.spark.sql.hive.llap \
            .MockHiveWarehouseSessionImpl.testFixture().getData().size()
        cls.RESULT_SIZE = session._jvm.com.hortonworks.spark.sql.hive.llap \
            .MockHiveWarehouseDataReader.RESULT_SIZE

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_execute_query(self):
        self.assertEqual(self.hive.executeQuery("SELECT * FROM t1").count(), self.RESULT_SIZE)

    def test_set_database(self):
        self.hive.setDatabase(TEST_DEFAULT_DB)

    def test_describe_table(self):
        self.assertEqual(self.hive.describeTable("testTable").count(), self.mockExecuteResultSize)

    def test_create_database(self):
        self.hive.createDatabase(TEST_DEFAULT_DB, False)
        self.hive.createDatabase(TEST_DEFAULT_DB, True)

    def test_show_table(self):
        self.assertEqual(self.hive.showTables().count(), self.mockExecuteResultSize)

    def test_create_table(self):
        CreateTableBuilder(self.spark, self.hive.createTable("TestTable")._jtablebuilder) \
            .ifNotExists() \
            .column("id", "int") \
            .column("val", "string") \
            .partition("id", "int") \
            .clusterBy(100, "val") \
            .prop("key", "value") \
            .create()


if __name__ == "__main__":
    unittest.main()
