# spark-llap

A library to load data into Spark SQL DataFrames from Hive using LLAP. It also contains Catalog/Context classes to enable querying of Hive tables without having to first register them as temporary tables in Spark SQL.

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is
automatically downloaded by the included shell script.
To build a JAR run 'build/sbt package' from the project root.

