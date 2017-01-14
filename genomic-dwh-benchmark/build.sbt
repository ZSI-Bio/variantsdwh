import sbt.Keys._

name := "genomic-dwh-benchmark"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.3" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.3" % "provided",
  "com.databricks" % "spark-csv_2.10" % "1.5.0",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.3" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
  "org.scalatest" % "scalatest_2.10" % "2.1.0-RC2" % "test",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "org.apache.commons" % "commons-math3" % "3.5",
  "org.rogach" %% "scallop" % "0.9.5"
)

resolvers += "Apache Repos" at "https://repository.apache.org/content/repositories/releases/"

assemblyMergeStrategy in assembly := {
  case PathList("com", "google", xs@_*) => MergeStrategy.first
  case PathList("edu", "umd", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("fi", "tkk", "ics", xs@_*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.first
  case PathList("org", "objectweb", xs@_*) => MergeStrategy.last
  case PathList("javax", "xml", xs@_*) => MergeStrategy.first
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("javax", "activation", xs@_*) => MergeStrategy.first
  case PathList("javax", "transaction", xs@_*) => MergeStrategy.first
  case PathList("javax", "mail", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case PathList("org", "apache", "hadoop", xs@_*) => MergeStrategy.first
  //case "META-INF/ECLIPSEF.RSA"     => MergeStrategy.discard
  case "META-INF/mimetypes.default" => MergeStrategy.first
  case ("META-INF/ECLIPSEF.RSA") => MergeStrategy.first
  case ("META-INF/mailcap") => MergeStrategy.first
  case ("plugin.properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}