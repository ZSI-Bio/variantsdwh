package pl.edu.pw.ii.zsibio.dwh.benchmark

import java.io.File
import java.nio.file.Path

import org.rogach.scallop.ScallopConf
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.JDBCDriver.JDBCDriver
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.{JDBCConnection, JDBCDriver}
import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.{DdlParser, KuduUtils, QueryExecutorWithLogging}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by marek on 14.01.17.
  */
object ExecuteStatement {

  class RunConf(args:Array[String]) extends ScallopConf(args){

    banner("Usage: ...")

    //val dbName = opt[String](required = false, descr = "Database name in HiveMetastore" )
    val useHive = opt[Boolean]("useHive",required = false, descr = "Create tables in Hive stored")
    val storageType = opt[String]("storageType",required = true, descr = "Storage type parquet|orc|kudu|carbon")
    val useImpala = opt[Boolean]("useImpala",required = false, descr = "Create tables in Kudu" )
    val usePresto = opt[Boolean]("usePresto",required = false, descr = "Create tables in Presto" )
    //val connString =opt[String]("connString",required = false, descr = "Connection string for SparlSQL Server" )
    //val kuduMaster =opt[String]("kuduMaster",required = false, descr = "Kudu Master URL" )
    //val compression =opt[String]("compression",required = false, default= Some("gzip"), descr = "Compression algorithm gzip|snappy|none" )
    //val username =opt[String]("username",required = false, descr = "Username" )
    //val password =opt[String]("password",required = false, descr = "Password" )
    val queryDir =opt[String]("queryDir",required = true, descr = "A file containing a select statement in YAML format" )
    val logFile =opt[String]("logFile",required = false, descr = "A file for storing timing results", default = Some("results.csv") )
    val partNum =opt[Int]("partNum",required = true, descr = "Number of partitions",default = Some(100) )

    verify()
  }

  def main(args: Array[String]) {

    val runConf = new RunConf(args)
    val confFile = ConfigFactory.load()
    val prestoConnString = confFile.getString("jdbc.presto.connection")
    val hiveConnString = confFile.getString("jdbc.hive.connection")
    val impalaConnString = confFile.getString("jdbc.impala.connection")
    val kuduMaster = confFile.getString("kudu.master.server")

    val jdbcConfArray = new ArrayBuffer[(JDBCDriver, String)]()

    if( runConf.useHive() && !hiveConnString.isEmpty)
      jdbcConfArray.append((JDBCDriver.HIVE,hiveConnString ) )
    else if (runConf.useHive() && hiveConnString.isEmpty)
      throw new Exception("Hive to be used but Hive jdbc is missing in the conf file")

    if (runConf.usePresto() && !prestoConnString.isEmpty)
      jdbcConfArray.append((JDBCDriver.PRESTO,prestoConnString ))
    else if (runConf.usePresto() && prestoConnString.isEmpty)
      throw new Exception("Hive to be used but Hive jdbc is missing in the conf file")

    if (runConf.useImpala() && !impalaConnString.isEmpty  &&
      ( (runConf.storageType().toLowerCase =="kudu" && !kuduMaster.isEmpty) || (runConf.storageType().toLowerCase() == "parquet") ) )
      jdbcConfArray.append((JDBCDriver.IMPALA,impalaConnString ) )
    else if (runConf.useImpala() && (impalaConnString.isEmpty  || kuduMaster.isEmpty) )
      throw new Exception("Kudu to be used but Impala jdbc or kuduMaster is missing in the conf file")

    jdbcConfArray.map( jobConf => {
        run(runConf, confFile, jobConf, kuduMaster)
      }
     )
    }

  def run(runConf:RunConf,confFile:Config,jobConf:(JDBCDriver,String), kuduMaster:String)={
    val conn = new JDBCConnection()
    conn.open(jobConf._1,jobConf._2)
    val allFiles = getRecursListFiles(new File(runConf.queryDir()))
        .filter(f => f.getName.endsWith("yaml"))
        .sortBy(f => f.getName)
    allFiles.map {queryFile =>
      val query = QueryExecutorWithLogging
            .parseQueryYAML(queryFile.getAbsolutePath, runConf.storageType(), jobConf._2, kuduMaster)
      if (query.queryType.toLowerCase() == "create" && !query.statement.toLowerCase().contains("create database")
        && query.storageFormat.toLowerCase() == "kudu") {
        val kuduUtils = new KuduUtils(kuduMaster)
        kuduUtils.createTable(query.statement, s"${runConf.storageType()}", true,
          confFile.getInt("kudu.table.partitions"), confFile.getInt("kudu.table.replication"))
      }
      QueryExecutorWithLogging.runStatement(query, conn, runConf.logFile())

    }
    conn.close
  }

  def getRecursListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(getRecursListFiles)
  }

}
