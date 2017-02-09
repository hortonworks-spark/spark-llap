# Apache Spark&trade; connector for Apache Hive&trade; LLAP

A library to load data into Apache Spark&trade; SQL DataFrames from
Apache Hive&trade; using LLAP. With Apache Ranger&trade;,
this library provides row/column level fine-grained access controls.

- **Shared Policies**: The data in a cluster can be shared securely and
  consistenly controlled by the shared access rules between Apache
  Spark&trade; and Apache Hive&trade;.

- **Audits**: All security activities can be monitored and searched
  in a single place, i.e., Apache Ranger&trade;

- **Resources**: Each user can use different queues while accessing the
  secured Hive data.


## Use cases

### Assumption

For all use cases, make it sure that the permission of Hive warehouse is 700.
It means non-`hive` user like `spark` can not access the secured tables.

```bash
$ hadoop fs -ls /apps/hive/
Found 1 items
drwx------   - hive hdfs          0 2017-02-01 20:52 /apps/hive/warehouse
```

In addition, make it sure that `hive.warehouse.subdir.inherit.perms=true`.
Newly created tables will inherit the permission by default.

### Case 1: Secure Spark Thrift Server with Fine-Grained Access

Run Spark Thrift Server with LLAP as `hive`. Then, Apache Ranger policies
rule Spark Thrift Server and Hive Thrift Server together seamlessly
as a single control center.

In the building section, we will describe
how to patch and how to build. For testing, refer the
[test document](https://github.com/hortonworks-spark/spark-llap/blob/master/src/test/python/README.md).

### Case 2: Shells (`spark-shell` or `pyspark`)

A non-Hive user also runs `spark-shell` or `pyspark` like the followings.
The user can see only the accessible data.

```bash
$ bin/spark-shell --jars spark-llap_2.11-1.0.3-2.1.jar --conf spark.sql.hive.llap=true
scala> sql("show databases").show()
+------------+
|databaseName|
+------------+
|    db_spark|
+------------+
```

```bash
$ bin/pyspark --jars spark-llap_2.11-1.0.3-2.1.jar --conf spark.sql.hive.llap=true
>>> sql("show databases").show()
+------------+
|databaseName|
+------------+
|    db_spark|
+------------+
```

### Case 3: Applications

A non-Hive user also can submit his spark job like the following.
Note that it will fail without `spark.sql.hive.llap=true` configuration.
You can find the full examples at `examples/src/main/python/spark_llap_sql.py`.

```python
spark = SparkSession \
    .builder \
    .appName("Spark LLAP SQL Python") \
    .master("yarn") \
    .enableHiveSupport() \
    .config("spark.sql.hive.llap", "true") \
    .getOrCreate()
spark.sql("show databases").show()
spark.sql("select * from db_spark.t_spark").show()
spark.stop()
```


## Prerequisites

### HiveServer2 Interactive (LLAP)

You need HiveServer2 Interactive service. For example, in HDP 2.5 or HDP 2.6 Preview,
navigate to `Hive` -> `Configs` -> `Settings` -> `Interactive Query`
and turn on `Enable Interactive Query (Tech Preview)`.


### Apache Ranger&trade;

If you want to use access control, you need to setup Apache Ranger policies.
The followings are some example policies.

#### Access policy for `db_spark` database

Name         | Table    | Column | Permissions
-------------|----------|--------|------------
spark_access | t_spark  | *      | Select

#### Masking policy in `db_spark` database

Name         | Table    | Column | Access Types | Select Masking Option
-------------|----------|--------|--------------|----------------------------
spark_mask   | t_spark  | name   | Select       | partial mask:'show first 4'

#### Filter policy `db_spark` database

Name         | Table    | Access Types | Row Level Filter
-------------|----------|--------------|-----------------
spark_filter | t_spark  | Select       | gender='M'

#### Access policy for `default` database (Optional)

This is for a temporary space for Spark while executing `INSERT INTO`.

Name         | Database | Table  | Column | Permissions
-------------|----------|--------|--------|------------
spark_system | default  | tmp_*  | *      | All


## Download or build `spark-llap` library

You can download the pre-built library at
https://github.com/hortonworks-spark/spark-llap/releases .

To build `spark-llap` from the source, do the following.

    git clone https://github.com/hortonworks-spark/spark-llap.git -b branch-2.1
    cd spark-llap
    build/sbt assembly


## Build Apache Spark&trade; 2.1.x with patch

    git clone https://github.com/apache/spark.git -b branch-2.1
    cd spark
    curl https://raw.githubusercontent.com/hortonworks-spark/spark-llap/branch-2.1/patch/0001-SPARK-LLAP-RANGER-Integration.patch | git am
    build/sbt -Pyarn -Phadoop-2.7 -Phive -Phive-thriftserver package


## Configuration

Copy your existing `hive-site.xml` and `spark-defaults.conf` into Apache Spark `conf` folder.

Add the following three configurations to `spark-defaults.conf`..

    spark.sql.hive.hiveserver2.url jdbc:hive2://YourHiveServer2URL:10500
    spark.hadoop.hive.llap.daemon.service.hosts *value for hive.llap.daemon.service.hosts in Hive configuration*
    spark.hadoop.hive.zookeeper.quorum *value for hive.zookeeper.quorum in Hive configuraion*


## Run

Start Spark Thrift Server with `spark.sql.hive.llap=true`.

    sbin/start-thriftserver.sh --jars spark-llap_2.11-1.0.3-2.1.jar --conf spark.sql.hive.llap=true 

You can turn off `spark-llap` by restarting Spark Thrift Server without this option or give `spark.sql.hive.llap=false`.

It is recommended to run Spark Thrift Server as user `hive` to use more SQL features.

You can access `spark-llap` enabled Spark Thrift Server via `beeline` or Apache Zeppelin&trade;.

    beeline -u jdbc:hive2://localhost:10016 -n hive -p password -e 'show databases'
    beeline -u jdbc:hive2://localhost:10016 -n spark -p password -e 'show tables'

There are two simple Python examples to submit jobs using Spark-LLAP.

    examples/src/main/python/spark_llap_sql.py
    examples/src/main/python/spark_llap_dsl.py

## Note for Kerberized Clusters

When using on kerberized clusters, add the followings into `Custom hive-interactive-site`.

    hive.llap.task.principal=hive/_HOST@EXAMPLE.COM
    hive.llap.task.keytab.file=/etc/security/keytabs/hive.service.keytab

You can access `spark-llap` enabled Spark Thrift Server via `beeline` or Apache Zeppelin&trade;.

    beeline -u "jdbc:hive2://hostname:10500/;principal=hive/_HOST@EXAMPLE.COM;hive.server2.proxy.user=hive" -p password -e 'show tables'
    beeline -u "jdbc:hive2://hostname:10500/;principal=hive/_HOST@EXAMPLE.COM;hive.server2.proxy.user=spark" -p password -e 'show tables'


