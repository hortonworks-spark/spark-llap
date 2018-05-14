# Apache Spark&trade; connector for Apache Hive&trade; LLAP

A library to load data into Apache Spark&trade; SQL DataFrames from
Apache Hive&trade; using LLAP. With Apache Ranger&trade;,
this library provides row/column level fine-grained access controls.

Usage
=====

Session Operations
------------------

* Start a HiveWarehouseConnector session (assuming the variable `spark` is an existing `SparkSession`):

`val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()`

* Set the current database for unqualified table references:

`hive.setDatabase("tpcds_bin_partitioned_orc_1000")`

Catalog Operations
------------------

* Show databases:

`hive.showDatabases().show(100, false)`

* Show tables:

`hive.showTables().show(100, false)`

* Describe table:

`hive.describeTable("web_sales").show(100, false)`

* Create a database:

`hive.createDatabase("spark_llap")`

* Create ORC table:

`hive.createTable(tempTable).ifNotExists().column("ws_sold_time_sk", "bigint").column("ws_ship_date_sk", "bigint").create()`

* Drop a database:

`hive.dropDatabase("spark_llap", isUsingIfExists, isUsingCascade)`

* Drop a table:

`hive.dropTable(tempTable, isUsingIfExists, isUsingPurge)`

Read Operations
---------------

* Execute Hive query and return DataFrame

`val df = hive.executeQuery("select * from web_sales")`

* Reference a Hive table as a DataFrame

`val df = hive.table("web_sales")`

* Execute Hive DDL and return DataFrame 

`hive.execute("describe extended web_sales").show(100, false)`

Write Operations
----------------

* LOAD a DataFrame to a Hive table

`df.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table", tempTable).option("loader", "batch").save()`

* Execute Hive update statement (with no results)

`hive.executeUpdate("ALTER TABLE old_name RENAME TO new_name")`

Example Script
--------------
Read table data from Hive, transform in Spark, write to new Hive table
 
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

HiveWarehouseSession Interface
==============================

	interface HiveWarehouseSession {
	
	    Dataset<Row> executeQuery(String sql);

	    //Shorthand for executeQuery 
	    Dataset<Row> q(String sql);

	    Dataset<Row> execute(String sql);

	    Dataset<Row> table(String sql);

	    SparkSession session();

	    void setDatabase(String name);

	    Dataset<Row> showDatabases();

	    Dataset<Row> showTables();

	    Dataset<Row> describeTable(String table);

	    void createDatabase(String database, boolean ifNotExists);

	    CreateTableBuilder createTable(String tableName);

	    void dropDatabase(String database, boolean ifExists, boolean cascade);

	    void dropTable(String table, boolean ifExists, boolean purge);
	}

