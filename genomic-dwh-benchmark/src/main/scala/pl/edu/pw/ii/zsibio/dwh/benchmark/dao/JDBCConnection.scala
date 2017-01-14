package pl.edu.pw.ii.zsibio.dwh.benchmark.dao

import java.io.IOException
import java.io.InputStream
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Properties

import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.ExecutionTiming;

/**
  * Created by marek on 14.01.17.
  */

case class QueryResult(rs:ResultSet, timing:Option[ExecutionTiming])

class JDBCConnection() {

  var connection: Connection = _

  /**
    * Open connection using a provided jdnbc driver and a connection string
    * @param jdbcDriverName
    * @param connectionUrl
    */
  def connect(jdbcDriverName:String, connectionUrl:String) = {
    Class.forName(jdbcDriverName)

    try {
      connection = DriverManager.getConnection(connectionUrl)
    }
    catch {
      case e: SQLException => e.printStackTrace()
      case e: Exception => e.printStackTrace()
    }
      finally
      {
        try {
          connection.close()
        } catch {
          case e: Exception => None
        }
      }
  }

  /**
    * Disconnect
    */
  def disconnect = connection.close()


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
