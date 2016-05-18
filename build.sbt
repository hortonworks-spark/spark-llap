
name := "spark-llap"
version := "1.0"
organization := "Hortonworks"
scalaVersion := "2.10.5"
val scalatestVersion = "2.2.4"

sparkVersion := "1.6.0"


val hadoopVersion = sys.props.getOrElse("hadoop.version", "2.7.1")
val hiveVersion = sys.props.getOrElse("hive.version", "2.1.0-SNAPSHOT")
val log4j2Version = sys.props.getOrElse("log4j2.version", "2.4.1")
val tezVersion = sys.props.getOrElse("tez.version", "0.8.3")
val thriftVersion = sys.props.getOrElse("thrift.version", "0.9.3")

spName := "Hortonworks/spark-llap"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

spIgnoreProvided := true

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "provided" force(),
  "org.apache.spark" %% "spark-catalyst" % testSparkVersion.value % "provided" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "provided" force(),

  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",

  ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "compile")
    .exclude("javax.servlet", "servlet-api")
    .exclude("stax", "stax-api"),

  ("org.apache.hadoop" % "hadoop-yarn-registry" % hadoopVersion % "compile")
    .exclude("commons-beanutils", "commons-beanutils")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("javax.servlet", "servlet-api")
    .exclude("stax", "stax-api"),

  ("org.apache.tez" % "tez-runtime-internals" % tezVersion % "compile")
    .exclude("javax.servlet", "servlet-api")
    .exclude("stax", "stax-api"),

  ("org.apache.hive" % "hive-llap-ext-client" % hiveVersion)
    .exclude("ant", "ant")
    .exclude("org.apache.ant", "ant")
    .exclude("org.apache.logging.log4j", "log4j-1.2-api")
    .exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
    .exclude("org.apache.slider", "slider-core")
    .exclude("stax", "stax-api")
    excludeAll(
      ExclusionRule(organization = "javax.servlet"),
      ExclusionRule(organization = "javax.servlet.jsp"),
      ExclusionRule(organization = "javax.transaction"),
      ExclusionRule(organization = "org.apache.hadoop"),
      ExclusionRule(organization = "org.datanucleus"),
      ExclusionRule(organization = "org.mortbay.jetty")
    )

)

// Assembly rules for shaded JAR
assemblyShadeRules in assembly := Seq(
  ShadeRule.zap("scala.**").inAll,

  // Relocate everything in Hive except for llap
  ShadeRule.rename("org.apache.hadoop.hive.ant.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.common.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.conf.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.io.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hive.jdbc.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.metastore.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.ql.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.serde.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.serde2.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.shims.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.thrift.**" -> "shadehive.@0").inAll
)
test in assembly := {}
assemblyMergeStrategy in assembly := {
  case PathList("org","apache","logging","log4j","core","config","plugins","Log4j2Plugins.dat") => MergeStrategy.first
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.discard
  case x if x.endsWith("package-info.class") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
val assemblyLogLevelString = sys.props.getOrElse("assembly.log.level", "error")
logLevel in assembly := {
  assemblyLogLevelString match {
    case "debug" => Level.Debug
    case "info" => Level.Info
    case "warn" => Level.Warn
    case "error" => Level.Error
  }
}

// Add assembly to publish task
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

// Get full stack trace
testOptions in Test += Tests.Argument("-oD")
fork := true
//javaOptions in (Test) += "-Xdebug"
//javaOptions in (Test) += "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
