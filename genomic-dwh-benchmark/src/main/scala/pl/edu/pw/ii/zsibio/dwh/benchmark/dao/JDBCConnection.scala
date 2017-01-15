package pl.edu.pw.ii.zsibio.dwh.benchmark.dao

import java.io.IOException
import java.io.InputStream
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Properties

import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.JDBCDriver.JDBCDriver
import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.ExecutionTiming

/**
  * Created by marek on 14.01.17.
  */

case class QueryResult(rs:ResultSet, timing:Option[ExecutionTiming])

object JDBCDriver extends Enumeration {

  type JDBCDriver = Value
  val HIVE, SPARK, IMPALA, PRESTO  = Value


}

class JDBCConnection() {

  var connection: Connection = _

  /**
    * Open connection using a provided jdnbc driver and a connection string
    * @param jdbcDriverName
    * @param connectionUrl
    */
  def open(jdbcDriverName:JDBCDriver, connectionUrl:String,userName:String="",password:String="") = {

    jdbcDriverName match {
      case JDBCDriver.HIVE => Class.forName("org.apache.hive.jdbc.HiveDriver")
      case JDBCDriver.SPARK => Class.forName("org.apache.hive.jdbc.HiveDriver")
      case JDBCDriver.IMPALA => Class.forName("com.cloudera.impala.jdbc41.Driver")
      case JDBCDriver.PRESTO => Class.forName("com.facebook.presto.jdbc.PrestoDriver")
      case _ => None
    }

    try {
      connection = DriverManager.getConnection(connectionUrl,userName,password)
    }
    catch {
      case e: SQLException => e.printStackTrace()
      case e: Exception => e.printStackTrace()
    }
      /*finally
      {
        try {
          connection.close()
        } catch {
          case e: Exception => None
        }
      }*/
  }

  /**
    * Disconnect
    */
  def close = connection.close()


  /**
    * Execute a query and
    * @param queryString
    * @param timing
    * @return
    */
  def executeQuery(queryString:String, timing:Boolean) : QueryResult= {


    val statement = connection.createStatement()
    val timer = if(timing) Some(new ExecutionTiming()) else None
    timer match{
      case Some(t) => t.startTimer
      case _ => None
    }
    val resultSet = statement.executeQuery(queryString)
    timer match{
      case Some(t) => t.stopTimer
      case _ => None
    }
    new QueryResult(resultSet,timer)
  }

  def executeUpdate(updateString:String) : Int ={
    val statement = connection.createStatement()
    val res = statement.executeUpdate(updateString)
    res
  }


}
