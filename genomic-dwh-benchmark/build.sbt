import sbt.Keys._

name := "genomic-dwh-benchmark"

version := "1.0"

scalaVersion := "2.10.6"

enablePlugins(UniversalPlugin)

fork := true

javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")

outputStrategy := Some(StdoutOutput)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.3" % "provided" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-sql_2.10" % "1.6.3" excludeAll ExclusionRule(organization = "javax.servlet") ,
  "com.databricks" % "spark-csv_2.10" % "1.5.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-hive_2.10" % "1.6.3"  excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.scalatest" % "scalatest_2.10" % "2.1.0-RC2" % "test",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "org.apache.commons" % "commons-math3" % "3.5",
  "org.rogach" %% "scallop" % "2.0.6",
  "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.8.2" excludeAll ExclusionRule(organization = "javax.servlet"),
  //"org.apache.hive" % "hive-service" % "1.1.0-cdh5.8.2" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.hadoop" % "hadoop-common" % "2.6.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "com.facebook.presto" % "presto-jdbc" % "0.163" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.logging.log4j" % "log4j-api" % "2.7",
  "net.jcazevedo" %% "moultingyaml" % "0.4.0",
  "org.apache.kudu" % "kudu-client" % "1.1.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.kudu" % "kudu-spark_2.10" % "1.1.0",
  "org.parboiled" %% "parboiled" % "2.1.3",
  "com.typesafe" % "config" % "1.3.1",
  "log4j" % "log4j" % "1.2.17"
)

resolvers += "Apache Repos" at "https://repository.apache.org/content/repositories/releases/"

resolvers += "CDH" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

mainClass in assembly := Some("pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement")


assemblyMergeStrategy in assembly := {
  case PathList("com", "google", xs@_*) => MergeStrategy.first
  case PathList("com", "sun", xs@_*) => MergeStrategy.first
  case PathList("com", "codahale", xs@_*) => MergeStrategy.first
  case PathList("edu", "umd", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("org", "jboss", xs@_*) => MergeStrategy.first
  case PathList("fi", "tkk", "ics", xs@_*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.first
  case PathList("org", "objectweb", xs@_*) => MergeStrategy.last
  case PathList("javax", "xml", xs@_*) => MergeStrategy.first
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("javax", "activation", xs@_*) => MergeStrategy.first
  case PathList("javax", "transaction", xs@_*) => MergeStrategy.first
  case PathList("javax", "mail", xs@_*) => MergeStrategy.first
  case PathList("javax", "jdo", xs@_*) => MergeStrategy.first
  case PathList("javax", "el", xs@_*) => MergeStrategy.first
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
  case PathList("shaded", "parquet", xs@_*) => MergeStrategy.first
  case PathList("com", "twitter", xs@_*) => MergeStrategy.first
  case PathList("org", "objenesis", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case PathList("org", "apache", "hadoop", xs@_*) => MergeStrategy.first
  //case "META-INF/ECLIPSEF.RSA"     => MergeStrategy.discard
  case "META-INF/mimetypes.default" => MergeStrategy.first
  case ("META-INF/ECLIPSEF.RSA") => MergeStrategy.first
  case ("META-INF/mailcap") => MergeStrategy.first
  case ("META-INF/jersey-module-version") => MergeStrategy.first
  case ("plugin.properties") => MergeStrategy.first
  case ("plugin.xml") => MergeStrategy.first
  case ("parquet.thrift") => MergeStrategy.first
  case ("hive-log4j.properties") => MergeStrategy.first
  case ("webapps/static/dt-1.9.4/css/demo_page.css") => MergeStrategy.first
  case ("webapps/static/dt-1.9.4/images/favicon.ico") => MergeStrategy.first
  case ("webapps/static/yarn.css") => MergeStrategy.first
  case ("webapps/static/yarn.dt.plugins.js") => MergeStrategy.first
  case ("images/ant_logo_large.gif") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
