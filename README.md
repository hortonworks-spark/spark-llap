
# HiveWarehouseConnector

A library to read/write DataFrames and Streaming DataFrames to/from
Apache Hive&trade; using LLAP. With Apache Ranger&trade;,
this library provides row/column level fine-grained access controls.

Compatibility
=====
Note that for open-source usage, master branch requires Hive 3.1.0 which is a
forthcoming release. For configuration of prior versions, please see [prior documentation](https://github.com/hortonworks-spark/spark-llap/wiki).

| branch | Spark | Hive  | HDP |
| ------------- |:-------------:|:-----:|-----:|
| master (Summer 2018) | 2.3.1 | 3.1.0 | 3.0.0 (GA) |
| branch-2.3 | 2.3.0 | 2.1.0 | 2.6.x (TP) |
| branch-2.2| 2.2.0 | 2.1.0 | 2.6.x (TP) |
| branch-2.1 | 2.1.1 | 2.1.0 | 2.6.x (TP) |
| branch-1.6 | 1.6.3 | 2.1.0 | 2.5.x (TP) |

Configuration
=====
Ensure the following Spark properties are set via `spark-defaults.conf` or
using `--conf` or through other Spark configuration.

| Property      | Description   | Example  |
| ------------- |:-------------:| -----:|
| spark.sql.hive.hiveserver2.jdbc.url | ThriftJDBC URL for LLAP HiveServer2 | jdbc:hive2://localhost:10000 |
| spark.datasource.hive.warehouse.load.staging.dir | Temp directory for batch writes to Hive | /tmp |
| spark.hadoop.hive.llap.daemon.service.hosts | App name for LLAP service | @llap0 |
| spark.hadoop.hive.zookeeper.quorum | Zookeeper hosts used by LLAP | host1:2181;host2:2181;host3:2181 |

For use in Spark client-mode on kerberized Yarn cluster, set:

| Property      | Description   | Example  |
| ------------- |:-------------:| -----:|
| spark.sql.hive.hiveserver2.jdbc.url.principal | Set equal to hive.server2.authentication.kerberos.principal  | hive/_HOST@EXAMPLE.COM |

For use in Spark cluster-mode on kerberized Yarn cluster, set:

| Property      | Description   | Example  |
| ------------- |:-------------:| -----:|
| spark.security.credentials.hiveserver2.enabled | Use Spark ServiceCredentialProvider | true |


Supported Types
=====
| Spark Type      | Hive Type               |
| --------------- | ----------------------- |
| ByteType        | TinyInt                 |
| ShortType       | SmallInt                |
| IntegerType     | Integer                 |
| LongType        | BigInt                  |
| FloatType       | Float                   |
| DoubleType      | Double                  |
| DecimalType     | Decimal                 |
| StringType\*    | String, Char, Varchar\* |
| BinaryType      | Binary                  |
| BooleanType     | Boolean                 |
| TimestampType\* | Timestamp\*             |
| DateType        | Date                    |
| ArrayType       | Array                   |
| StructType      | Struct                  |

- A Hive String, Char, Varchar column will be converted into a Spark StringType column.
- When a Spark StringType column has maxLength metadata, it will be converted into a Hive Varchar column. Otherwise, it will be converted into a Hive String column.
- A Hive Timestamp column will lose sub-microsecond precision when it is converted into a Spark TimestampType column. Because a Spark TimestampType column is microsecond precision, while a Hive Timestamp column is nanosecond precision.

Unsupported Types
=====
| Spark Type           | Hive Type | Plan                        |
| -------------------- | --------- | --------------------------- |
| CalendarIntervalType | Interval  | Planned for future support  |
| MapType              | Map       | Planned for future support  |
| N/A                  | Union     | Not supported in Spark      |
| NullType             | N/A       | Not supported in Hive       |

Submitting Applications
=====
Support is currently available for `spark-shell`, `pyspark`, and `spark-submit`.

Scala/Java usage:
-----

1. Locate the `hive-warehouse-connector-assembly` jar.
If building from source, this will be located within the `target/scala-2.11` folder.
If using pre-built distro, follow instructions from your distro provider, e.g. on HDP
the jar would be located in `/usr/hdp/current/hive-warehouse-connector/`

2. Use `--jars` to add the connector jar to app submission, e.g.

`spark-shell --jars /usr/hdp/current/hive-warehouse-connector/hive-warehouse-connector-assembly-1.0.0.jar`

Python usage:
-----

1. Follow the instructions above to add the connector jar to app submission.
2. Additionally add the connector's Python package to app submission, e.g.

`pyspark --jars /usr/hdp/current/hive-warehouse-connector/hive-warehouse-connector-assembly-1.0.0.jar
         --py-files /usr/hdp/current/hive-warehouse-connector/pyspark_hwc-1.0.0.zip`

API Usage
=====

Session Operations
------------------

`HiveWarehouseSession` acts as an API to bridge Spark with HiveServer2.
In your Spark source, create an instance of `HiveWarehouseSession` using `HiveWarehouseBuilder`

* Create HiveWarehouseSession (assuming `spark` is an existing `SparkSession`):

`val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()`

* Set the current database for unqualified Hive table references:

`hive.setDatabase(<database>)`

Catalog Operations
------------------

* Execute catalog operation and return DataFrame, e.g.

`hive.execute("describe extended web_sales").show(100, false)`

* Show databases:

`hive.showDatabases().show(100, false)`

* Show tables for current database:

`hive.showTables().show(100, false)`

* Describe table:

`hive.describeTable(<table_name>).show(100, false)`

* Create a database:

`hive.createDatabase(<database_name>)`

* Create ORC table, e.g.:

`hive.createTable("web_sales")
     .ifNotExists()
     .column("sold_time_sk", "bigint")
     .column("ws_ship_date_sk", "bigint")
     .create()`

* Drop a database:

`hive.dropDatabase(<databaseName>, <ifExists>, <useCascade>)`

* Drop a table:

`hive.dropTable(<tableName>, <ifExists>, <usePurge>)`

Read Operations
---------------

* Execute Hive SELECT query and return DataFrame, e.g.

`val df = hive.executeQuery("select * from web_sales")`

* Reference a Hive table as a DataFrame

`val df = hive.table(<tableName>)`

Write Operations
----------------

* Execute Hive update statement, e.g.

`hive.executeUpdate("ALTER TABLE old_name RENAME TO new_name")`

* Write a DataFrame to Hive in batch (uses LOAD DATA INTO TABLE), e.g.

`df.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector")
   .option("table", <tableName>)
   .save()`

* Write a DataFrame to Hive using HiveStreaming, e.g.

```
  df.write.format("com.hortonworks.spark.sql.hive.llap.HiveStreamingDataSource")
   .option("database", <databaseName>)
   .option("table", <tableName>)
   .option("metastoreUri", <HMS_URI>)
   .save()

 // To write to static partition
 df.write.format("com.hortonworks.spark.sql.hive.llap.HiveStreamingDataSource")
   .option("database", <databaseName>)
   .option("table", <tableName>)
   .option("partition", <partition>)
   .option("metastoreUri", <HMS URI>)
   .save()
```

* Write a Spark Stream to Hive using HiveStreaming, e.g.
```
stream.writeStream
    .format("com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource")
    .option("metastoreUri", metastoreUri)
    .option("database", "streaming")
    .option("table", "web_sales")
    .start()
```

HiveWarehouseSession Interface
==============================
```
	interface HiveWarehouseSession {

        //Execute Hive SELECT query and return DataFrame
	    Dataset<Row> executeQuery(String sql);

        //Execute Hive update statement
	    boolean executeUpdate(String sql);

        //Execute Hive catalog-browsing operation and return DataFrame
	    Dataset<Row> execute(String sql);

        //Reference a Hive table as a DataFrame
	    Dataset<Row> table(String sql);

        //Return the SparkSession attached to this HiveWarehouseSession
	    SparkSession session();

        //Set the current database for unqualified Hive table references
	    void setDatabase(String name);

        /**
         * Helpers: wrapper functions over execute or executeUpdate
         */

        //Helper for show databases
	    Dataset<Row> showDatabases();

        //Helper for show tables
	    Dataset<Row> showTables();

        //Helper for describeTable
	    Dataset<Row> describeTable(String table);

        //Helper for create database
	    void createDatabase(String database, boolean ifNotExists);

        //Helper for create table stored as ORC
	    CreateTableBuilder createTable(String tableName);

        //Helper for drop database
	    void dropDatabase(String database, boolean ifExists, boolean cascade);

        //Helper for drop table
	    void dropTable(String table, boolean ifExists, boolean purge);
	}
```

Batch Load Example
====
Read table data from Hive, transform in Spark, write to new Hive table
```
	val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()
	hive.setDatabase("tpcds_bin_partitioned_orc_1000")
	val df = hive.executeQuery("select * from web_sales")
	hive.setDatabase("spark_llap")
	val tempTable = "t_" + System.currentTimeMillis()
	hive.createTable(tempTable).ifNotExists().column("ws_sold_time_sk", "bigint").column("ws_ship_date_sk", "bigint").create()
	df.select("ws_sold_time_sk", "ws_ship_date_sk").filter("ws_sold_time_sk > 80000").write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table", tempTable).save()
	val df2 = hive.executeQuery("select * from " + tempTable)
	df2.show(20)
	hive.dropTable(tempTable, true, false)
```
