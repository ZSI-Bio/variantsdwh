package pl.edu.pw.ii.zsibio.dwh.benchmark

import org.rogach.scallop.ScallopConf
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.JDBCDriver.JDBCDriver
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.{JDBCConnection, JDBCDriver}
import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.{DdlParser, KuduUtils, QueryExecutorWithLogging}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by marek on 14.01.17.
  */
object ExecuteStatement {



  def main(args: Array[String]) {
    object Conf extends ScallopConf(args){

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
      val queryFile =opt[String]("queryFile",required = true, descr = "A file containing a select statement in YAML format" )
      val logFile =opt[String]("logFile",required = true, descr = "A file for storing timing results" )
      val partNum =opt[Int]("partNum",required = true, descr = "Number of partitions",default = Some(100) )

      verify()
    }

    val confFile = ConfigFactory.load()
    val prestoConnString = confFile.getString("jdbc.presto.connection")
    val hiveConnString = confFile.getString("jdbc.hive.connection")
    val impalaConnString = confFile.getString("jdbc.impala.connection")
    val kuduMaster = confFile.getString("kudu.master.server")

    val jdbcConfArray = new ArrayBuffer[(JDBCDriver, String)]()

    if( Conf.useHive() && !hiveConnString.isEmpty)
      jdbcConfArray.append((JDBCDriver.HIVE,hiveConnString ) )
    else if (Conf.useHive() && hiveConnString.isEmpty)
      throw new Exception("Hive to be used but Hive jdbc is missing in the conf file")

    if (Conf.usePresto() && !prestoConnString.isEmpty)
      jdbcConfArray.append((JDBCDriver.PRESTO,prestoConnString ))
    else if (Conf.usePresto() && prestoConnString.isEmpty)
      throw new Exception("Hive to be used but Hive jdbc is missing in the conf file")

    if (Conf.useImpala() && !impalaConnString.isEmpty  &&
      ( (Conf.storageType().toLowerCase =="kudu" && !kuduMaster.isEmpty) || (Conf.storageType().toLowerCase() == "parquet") ) )
      jdbcConfArray.append((JDBCDriver.IMPALA,impalaConnString ) )
    else if (Conf.useImpala() && (impalaConnString.isEmpty  || kuduMaster.isEmpty) )
      throw new Exception("Kudu to be used but Impala jdbc or kuduMaster is missing in the conf file")

    jdbcConfArray.map( jobConf=> {
        val conn = new JDBCConnection()
        conn.open(jobConf._1,jobConf._2)
        Conf.queryFile.toOption match{

          case Some(file) => {
            val query = QueryExecutorWithLogging
              .parseQueryYAML(file,Conf.storageType(),jobConf._2,kuduMaster)

            if(query.queryType.toLowerCase() =="create" && query.storageFormat.toLowerCase()=="kudu"){
              val kuduUtils = new KuduUtils(kuduMaster)
              kuduUtils.createTable(query.statement,s"${Conf.storageType()}",true,20,1)
              }


            QueryExecutorWithLogging.runStatement(query,conn,Conf.logFile())
          }
          case _ => None
        }
        conn.close
      }
     )


    }

}
