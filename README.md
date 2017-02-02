# Apache Spark&trade; connector for Apache Hive&trade; LLAP

A library to load data into Apache Spark&trade; SQL DataFrames from
Apache Hive&trade; using LLAP. With Apache Ranger&trade; (Incubating),
this library provides row/column level fine-grained access controls
via Spark Thrift Server.

In other words, the data in a cluster can be shared securely and
consistenly controlled by the shared access rules between Apache
Spark&trade; and Apache Hive&trade;.

## Prerequisites

### HiveServer2 Interactive (LLAP)

You need HiveServer2 Interactive service. For example, in HDP 2.5 or HDP 2.6 Preview,
navigate to `Hive` -> `Configs` -> `Settings` -> `Interactive Query`
and turn on `Enable Interactive Query (Tech Preview)`.


### Apache Ranger&trade;

If you want to use access control, you need to setup Apache Ranger policies.
The followings are some example policies.

#### Access policy for `spark_db` database

Name         | Table    | Column | Permissions
-------------|----------|--------|------------
spark_access | t_sample | *      | Select

#### Masking policy in `spark_db` database

Name         | Table    | Column | Access Types | Select Masking Option
-------------|----------|--------|--------------|----------------------------
spark_mask   | t_sample | name   | Select       | partial mask:'show first 4'

#### Filter policy `spark_db` database

Name         | Table    | Access Types | Row Level Filter
-------------|----------|--------------|-----------------
spark_filter | t_sample | Select       | gender='M'

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

It is recommended to run Spark Thrift Server as user `hive`.

You can access `spark-llap` enabled Spark Thrift Server via `beeline` or Apache Zeppelin&trade;.

    beeline -u jdbc:hive2://localhost:10016 -n hive -p password -e 'show databases'
    beeline -u jdbc:hive2://localhost:10016 -n spark -p password -e 'show tables'


## Note for Kerberized Clusters

When using on kerberized clusters, add the followings into `Custom hive-interactive-site`.

    hive.llap.task.principal=hive/_HOST@EXAMPLE.COM
    hive.llap.task.keytab.file=/etc/security/keytabs/hive.service.keytab

You can access `spark-llap` enabled Spark Thrift Server via `beeline` or Apache Zeppelin&trade;.

    beeline -u "jdbc:hive2://hostname:10500/;principal=hive/_HOST@EXAMPLE.COM;hive.server2.proxy.user=hive" -p password -e 'show tables'
    beeline -u "jdbc:hive2://hostname:10500/;principal=hive/_HOST@EXAMPLE.COM;hive.server2.proxy.user=spark" -p password -e 'show tables'

