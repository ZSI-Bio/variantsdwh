package pl.edu.pw.ii.zsibio.dwh.benchmark

import org.rogach.scallop.ScallopConf
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.JDBCDriver.JDBCDriver
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.{JDBCConnection, JDBCDriver}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by marek on 14.01.17.
  */
object ExecuteStatement {



  def main(args: Array[String]) {
    object Conf extends ScallopConf(args){

      banner("Usage: ...")

      val dbName = opt[String](required = false, descr = "Database name in HiveMetastore" )
      val useHiveOrc = opt[Boolean]("useHiveOrc",required = false, descr = "Create tables in Hive stored as ORC file format" )
      val useHivePq = opt[Boolean](required = false, descr = "Create tables in Hive stored as Parquet file format" )
      val useKudu = opt[Boolean]("useKudu",required = false, descr = "Create tables in Kudu" )
      val usePresto = opt[Boolean]("usePresto",required = false, descr = "Create tables in Presto" )
      val sparkConnstring =opt[String](required = false, descr = "Connection string for SparlSQL Server" )
      val hiveConnstring =opt[String]("hiveConnstring",required = false, descr = "Connection string for Hive Server" )
      val prestoConnstring =opt[String]("prestoConnstring",required = false, descr = "Connection string for Presto Server" )
      val impalaConnstring =opt[String]("impalaConnstring",required = false, descr = "Connection string for Impala Server" )
      val kuduMaster =opt[String]("kuduMaster",required = false, descr = "Kudu Master URL" )
      val compression =opt[String]("compression",required = false, default= Some("gzip"), descr = "Compression algorithm gzip|snappy|none" )
      val username =opt[String]("username",required = false, descr = "Username" )
      val password =opt[String]("password",required = false, descr = "Password" )
      val queryFile =opt[String]("queryFile",required = false, descr = "A file containing a select statement in YAML format" )
      val ddlFile =opt[String]("ddlFile",required = false, descr = "A file containing a DDL statement in YAML format" )

      verify()
    }
    val jdbcConfArray = new ArrayBuffer[(JDBCDriver, String)]()

    if( (Conf.useHiveOrc() ||  Conf.useHivePq()) && Conf.hiveConnstring.toOption != None)
      jdbcConfArray.append((JDBCDriver.HIVE,Conf.hiveConnstring() ) )

    if (Conf.usePresto() && Conf.prestoConnstring.toOption != None)
      jdbcConfArray.append((JDBCDriver.PRESTO,Conf.prestoConnstring()) )

    if (Conf.useKudu() && Conf.impalaConnstring.toOption != None)
      jdbcConfArray.append((JDBCDriver.IMPALA,Conf.impalaConnstring() ) )

    jdbcConfArray.map( jobConf=> {
        val conn = new JDBCConnection()
        conn.open(jobConf._1,jobConf._2,Conf.username(),Conf.password() )
        val res = conn.executeQuery("SELECT 1", true)
        println(s"${jobConf._1.toString}: ${res.timing.get.getTiming()}")
        conn.close
      }
     )


    }

}
