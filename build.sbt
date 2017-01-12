
name := "spark-llap_2.11"
version := "1.0.1-2.1"
organization := "com.hortonworks"
scalaVersion := "2.11.8"
val scalatestVersion = "2.2.6"

sparkVersion := sys.props.getOrElse("spark.version", "2.1.0")

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

val hadoopVersion = sys.props.getOrElse("hadoop.version", "2.7.3")
val hiveVersion = sys.props.getOrElse("hive.version", "2.1.0.2.5.3.0-37")
val log4j2Version = sys.props.getOrElse("log4j2.version", "2.4.1")
val tezVersion = sys.props.getOrElse("tez.version", "0.8.4")
val thriftVersion = sys.props.getOrElse("thrift.version", "0.9.3")
val repoUrl = sys.props.getOrElse("repourl", "https://repo1.maven.org/maven2/")

spName := "hortonworks/spark-llap"

// disable using the Scala version in output paths and artifacts
crossPaths := false

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

spIgnoreProvided := true

checksums in update := Nil

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "provided" force(),
  "org.apache.spark" %% "spark-catalyst" % testSparkVersion.value % "provided" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "provided" force(),
  "org.apache.spark" %% "spark-hive" % testSparkVersion.value % "provided" force(),

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
    .exclude("org.apache.logging.log4j", "log4j-web")
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
dependencyOverrides += "com.google.guava" % "guava" % "16.0.1"
dependencyOverrides += "commons-codec" % "commons-codec" % "1.6"
dependencyOverrides += "commons-logging" % "commons-logging" % "1.2"

// Assembly rules for shaded JAR
assemblyShadeRules in assembly := Seq(
  ShadeRule.zap("scala.**").inAll,

  // Relocate everything in Hive except for llap
  ShadeRule.rename("org.apache.hadoop.hive.ant.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.common.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.conf.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.io.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hive.jdbc.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hive.service.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.metastore.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.ql.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.serde.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.serde2.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.shims.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.thrift.**" -> "shadehive.@0").inAll,

  ShadeRule.rename("org.apache.derby.**" -> "shadederby.@0").inAll
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
resolvers += "Additional Maven Repository" at repoUrl
resolvers += "Hortonworks Maven Repository" at "http://repo.hortonworks.com/content/groups/public/"

// Get full stack trace
testOptions in Test += Tests.Argument("-oD")
fork := true
