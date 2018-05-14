# Apache Spark&trade; connector for Apache Hive&trade; LLAP

A library to load data into Apache Spark&trade; SQL DataFrames from
Apache Hive&trade; using LLAP. With Apache Ranger&trade;,
this library provides row/column level fine-grained access controls.

Usage
=====

* Start a HiveWarehouseConnector session:

`val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()`

* Start a HiveWarehouseConnector session:

`val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()`
