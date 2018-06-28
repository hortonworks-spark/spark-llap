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

import sys

if sys.version >= '3':
    basestring = unicode = str
    long = int

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame


class HiveWarehouseBuilder(object):
    """Builder for :class:`HiveWarehouseSession`."""

    def __init__(self, spark_session, jhwbuilder):
        self._spark_session = spark_session
        self._jhwbuilder = jhwbuilder

    @staticmethod
    def session(session):
        assert isinstance(session, SparkSession), "session should be a spark session"

        jsparkSession = session._jsparkSession
        jvm = session._jvm

        return HiveWarehouseBuilder(
            session,
            jvm.com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(jsparkSession))

    def userPassword(self, user, password):
        assert isinstance(user, basestring), "user should be a string"
        assert isinstance(password, basestring), "password should be a string"

        self._jhwbuilder.userPassword(user, password)
        return self

    def hs2url(self, hs2url):
        assert isinstance(hs2url, basestring), "hs2url should be a string"

        self._jhwbuilder.hs2url(hs2url)
        return self

    def maxExecResults(self, maxExecResults):
        assert isinstance(maxExecResults, int), "maxExecResults should be a number"

        self._jhwbuilder.maxExecResults(maxExecResults)
        return self

    def dbcp2Conf(self, dbcp2Conf):
        assert isinstance(dbcp2Conf, basestring), "dbcp2Conf should be a string"

        self._jhwbuilder.dbcp2Conf(dbcp2Conf)
        return self

    def defaultDB(self, defaultDB):
        assert isinstance(defaultDB, basestring), "defaultDB should be a string"

        self._jhwbuilder.defaultDB(defaultDB)
        return self

    def principal(self, principal):
        assert isinstance(principal, basestring), "principal should be a string"

        self._jhwbuilder.principal(principal)
        return self

    def credentialsEnabled(self):
        self._jhwbuilder.credentialsEnabled()
        return self

    def build(self):
        return HiveWarehouseSessionImpl(self._spark_session, self._jhwbuilder.build())


class HiveWarehouseSessionImpl(object):
    """This only can be created via HiveWarehouseSession.session(...).build()."""

    def __init__(self, spark_session, jhwsession):
        self._spark_session = spark_session
        self._jhwsession = jhwsession

    def executeQuery(self, sql):
        assert isinstance(sql, basestring), "sql should be a string"

        return DataFrame(self._jhwsession.executeQuery(sql), self._spark_session._wrapped)

    def q(self, sql):
        assert isinstance(sql, basestring), "sql should be a string"

        return DataFrame(self._jhwsession.q(sql), self._spark_session._wrapped)

    def execute(self, sql):
        assert isinstance(sql, basestring), "sql should be a string"

        return DataFrame(self._jhwsession.execute(sql), self._spark_session._wrapped)

    def table(self, sql):
        assert isinstance(sql, basestring), "sql should be a string"

        return DataFrame(self._jhwsession.table(sql), self._spark_session._wrapped)

    def session(self):
        return self._spark_session

    def setDatabase(self, name):
        assert isinstance(name, basestring), "name should be a string"

        self._jhwsession.setDatabase(name)

    def showDatabases(self):
        return DataFrame(self._jhwsession.showDatabases(), self._spark_session._wrapped)

    def showTables(self):
        return DataFrame(self._jhwsession.showTables(), self._spark_session._wrapped)

    def describeTable(self, table):
        assert isinstance(table, basestring), "table should be a string"

        return DataFrame(self._jhwsession.describeTable(table), self._spark_session._wrapped)

    def createDatabase(self, database, ifNotExists):
        assert isinstance(ifNotExists, bool), "ifNotExists should be a bool"
        assert isinstance(database, basestring), "database should be a string"

        self._jhwsession.createDatabase(database, ifNotExists)

    def createTable(self, tableName):
        assert isinstance(tableName, basestring), "table should be a string"

        return CreateTableBuilder(self._spark_session, self._jhwsession.createTable(tableName))

    def dropDatabase(self, database, ifExists, cascade):
        assert isinstance(database, basestring), "database should be a string"
        assert isinstance(ifExists, bool), "ifExists should be a bool"
        assert isinstance(cascade, bool), "cascade should be a bool"

        self._jhwsession.dropDatabase(database, ifExists, cascade)

    def dropTable(self, table, ifExists, purge):
        assert isinstance(table, basestring), "table should be a string"
        assert isinstance(ifExists, bool), "ifExists should be a bool"
        assert isinstance(purge, bool), "purge should be a bool"

        self._jhwsession.dropTable(table, ifExists, purge)


class CreateTableBuilder(object):
    """This only can be created via HiveWarehouseBuilder.session(...).build().createTable(...)."""

    def __init__(self, spark_session, jtablebuilder):
        self._spark_session = spark_session
        self._jtablebuilder = jtablebuilder

    def ifNotExists(self):
        self._jtablebuilder.ifNotExists()
        return self

    def column(self, name, type):
        assert isinstance(name, basestring), "name should be a string"
        assert isinstance(type, basestring), "type should be a string"

        self._jtablebuilder.column(name, type)
        return self

    def partition(self, name, type):
        assert isinstance(name, basestring), "name should be a string"
        assert isinstance(type, basestring), "type should be a string"

        self._jtablebuilder.partition(name, type)
        return self

    def prop(self, key, value):
        assert isinstance(key, basestring), "key should be a string"
        assert isinstance(value, basestring), "value should be a string"

        self._jtablebuilder.prop(key, value)
        return self

    def clusterBy(self, numBuckets, *columns):
        assert isinstance(numBuckets, int), "numBuckets should be a number"
        assert isinstance(columns, (list, tuple)) and isinstance(columns[0], basestring), \
            "columns should be a list of strings"

        # Create an array of objects
        str_class = self._spark_session._jvm.String
        arr = SparkContext._gateway.new_array(str_class, len(columns))
        for i in range(len(columns)):
            arr[i] = columns[i]
        self._jtablebuilder.clusterBy(numBuckets, arr)
        return self

    def create(self):
        self._jtablebuilder.create()

    def toString(self):
        return str(self._jtablebuilder.toString())


class HiveWarehouseSession(object):
    """Public interface for Spark LLAP."""
    HIVE_WAREHOUSE_CONNECTOR = "com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector"
    DATAFRAME_TO_STREAM = "com.hortonworks.spark.sql.hive.llap.HiveStreamingDataSource"
    STREAM_TO_STREAM = "com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource"

    @staticmethod
    def session(session):
        return HiveWarehouseBuilder.session(session)
