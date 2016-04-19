
name := "spark-llap"
version := "1.0"
organization := "Hortonworks"
scalaVersion := "2.10.5"
//scalaVersion := "2.11.4"
//crossScalaVersions := Seq("2.10.5", "2.11.4")
val scalatestVersion = "2.2.4"

sparkVersion := "1.6.0"
//sparkComponents ++= Seq("core", "catalyst", "sql", "hive")
//sparkComponents ++= Seq("core", "catalyst", "sql")

val hiveVersion = "2.1.0-SNAPSHOT"
val hadoopVersion = "2.7.1"
val tezVersion = "0.8.2"

spName := "Hortonworks/spark-llap"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)


libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % testSparkVersion.value, //% "test" force(),
  "org.apache.spark" %% "spark-catalyst" % testSparkVersion.value, //% "test" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value, //% "test" force(),
//  "org.apache.spark" %% "spark-hive" % testSparkVersion.value, //% "test" force(),


  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",

  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "compile" excludeAll(
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  ),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "compile" excludeAll(
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  ),
  "org.apache.tez" % "tez-api" % tezVersion % "test" excludeAll(
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  ),
  "org.apache.tez" % "tez-runtime-internals" % tezVersion % "test" excludeAll(
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  ),

  "org.apache.hive" % "hive-common" % hiveVersion  excludeAll(
    ExclusionRule(organization = "com.sun.jersey"),
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  ),
  "org.apache.hive" % "hive-jdbc" % hiveVersion  excludeAll(
    ExclusionRule(organization = "com.sun.jersey"),
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  ),
  "org.apache.hive" % "hive-exec" % hiveVersion  excludeAll(
    ExclusionRule(organization = "com.sun.jersey"),
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  ),
  "org.apache.hive" % "hive-llap-server" % hiveVersion  excludeAll(
    ExclusionRule(organization = "com.sun.jersey"),
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  ),
  "org.apache.hive" % "hive-metastore" % hiveVersion excludeAll(
    ExclusionRule(organization = "com.sun.jersey"),
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  ),
  "commons-logging" % "commons-logging" % "1.2"
)

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

// Get full stack trace
testOptions in Test += Tests.Argument("-oD")
fork := true
//javaOptions in (Test) += "-Xdebug"
//javaOptions in (Test) += "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
