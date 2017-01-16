package pl.edu.pw.ii.zsibio.dwh.benchmark

import org.rogach.scallop.ScallopConf
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.JDBCDriver.JDBCDriver
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.{JDBCConnection, JDBCDriver}
import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.QueryExecutorWithLogging

import scala.collection.mutable.ArrayBuffer

/**
  * Created by marek on 14.01.17.
  */
object ExecuteStatement {



  def main(args: Array[String]) {
    object Conf extends ScallopConf(args){

      banner("Usage: ...")

      //val dbName = opt[String](required = false, descr = "Database name in HiveMetastore" )
      val useHive = opt[Boolean]("useHive",required = false, descr = "Create tables in Hive stored")
      val storageType = opt[String]("storageType",required = true, descr = "Storage type parquet|orc|kudu")
      val useKudu = opt[Boolean]("useKudu",required = false, descr = "Create tables in Kudu" )
      val usePresto = opt[Boolean]("usePresto",required = false, descr = "Create tables in Presto" )
      val connString =opt[String]("connString",required = false, descr = "Connection string for SparlSQL Server" )
      val kuduMaster =opt[String]("kuduMaster",required = false, descr = "Kudu Master URL" )
      val compression =opt[String]("compression",required = false, default= Some("gzip"), descr = "Compression algorithm gzip|snappy|none" )
      val username =opt[String]("username",required = false, descr = "Username" )
      val password =opt[String]("password",required = false, descr = "Password" )
      val queryFile =opt[String]("queryFile",required = true, descr = "A file containing a select statement in YAML format" )
      val logFile =opt[String]("logFile",required = true, descr = "A file for storing timing results" )
      verify()
    }
    val jdbcConfArray = new ArrayBuffer[(JDBCDriver, String)]()

    if( Conf.useHive() && Conf.connString.toOption != None)
      jdbcConfArray.append((JDBCDriver.HIVE,Conf.connString() ) )

    if (Conf.usePresto() && Conf.connString.toOption != None)
      jdbcConfArray.append((JDBCDriver.PRESTO,Conf.connString()) )

    if (Conf.useKudu() && Conf.connString.toOption != None)
      jdbcConfArray.append((JDBCDriver.IMPALA,Conf.connString() ) )

    jdbcConfArray.map( jobConf=> {
        val conn = new JDBCConnection()
        conn.open(jobConf._1,jobConf._2)
        Conf.queryFile.toOption match{

          case Some(file) => {
            def replaceVars(property:String) ={
              property
                .replaceAll("\\{\\{DATA_FORMAT\\}\\}",Conf.storageType().toUpperCase)
                .replaceAll("\\{\\{DB_NAME\\}\\}",Conf.connString().split('/').last.toUpperCase)
            }
            val query = QueryExecutorWithLogging.parseQuertYAML(file)
            val queryWithReplacedVars = query.copy(
              queryId = replaceVars(query.queryId),
              queryDesc = replaceVars(query.queryDesc),
              storageFormat = replaceVars(query.storageFormat),
              statement = replaceVars(query.statement)
                           )
            QueryExecutorWithLogging.runStatement(queryWithReplacedVars,conn,Conf.logFile())
          }
          case _ => None
        }
        conn.close
      }
     )


    }

}
