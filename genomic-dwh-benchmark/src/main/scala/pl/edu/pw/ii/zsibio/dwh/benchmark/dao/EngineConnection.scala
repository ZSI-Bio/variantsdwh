package pl.edu.pw.ii.zsibio.dwh.benchmark.dao

import java.io.IOException
import java.io.InputStream
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Properties

import com.cloudera.beeswax.api.Query
import io.netty.handler.codec.http.QueryStringEncoder
import org.apache.impala.thrift.ImpalaService
import org.apache.impala.thrift.ImpalaService.Client
//import org.apache.apache.impala.thirft._
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.ConnectDriver.Driver
import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.ExecutionTiming

/**
  * Created by marek on 14.01.17.
  */

case class QueryResult(rs:ResultSet, timing:Option[ExecutionTiming])

object ConnectDriver extends Enumeration {

  type Driver = Value
  val HIVE, SPARK, IMPALA_JDBC, IMPALA_THRIFT, PRESTO, SPARK1, SPARK2, UKNOWN  = Value

}

class EngineConnection(driver: Driver) {

  val connectDriver = driver
  var connection: Connection = _
  var impalaClient : Client  =_

  /**
    * Open connection using a provided jdnbc driver and a connection string
    * @param driverName
    * @param connectionUrl
    */
  def open(driverName:Driver, connectionUrl:String, userName:String="", password:String="") = {

    driverName match {
      case ConnectDriver.HIVE => Class.forName("org.apache.hive.jdbc.HiveDriver")
      case ConnectDriver.SPARK => Class.forName("org.apache.hive.jdbc.HiveDriver")
      case ConnectDriver.IMPALA_JDBC => Class.forName("com.cloudera.impala.jdbc41.Driver")
      case ConnectDriver.PRESTO => Class.forName("com.facebook.presto.jdbc.PrestoDriver")
      case _ => None
    }

    try {
      if(connectDriver != ConnectDriver.IMPALA_THRIFT)
        connection = DriverManager.getConnection(connectionUrl,userName,password)
      else {
        val transport = new TSocket("quickstart.cloudera",21000)
        transport.open()
        val protocol = new TBinaryProtocol(transport)
        impalaClient = new ImpalaService.Client(protocol)
        impalaClient.PingImpalaService()
      }
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
  def close = {
    if( connectDriver != ConnectDriver.IMPALA_THRIFT)
      connection.close()
    else
      impalaClient.getInputProtocol.getTransport.close()
  }


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
    if(connectDriver == ConnectDriver.HIVE){

      statement.execute("SET mapreduce.map.java.opts=-Xmx3072m")
      statement.execute("set mapreduce.reduce.java.opts=-Xmx3072m")
    }
    val resultSet = statement.executeQuery(queryString)
    while (resultSet.next()){}
    timer match{
      case Some(t) => t.stopTimer
      case _ => None
    }
    new QueryResult(resultSet,timer)
  }

  def executeUpdate(updateString:String) : Int ={

    if(connectDriver != ConnectDriver.IMPALA_THRIFT) {
      val statement = connection.createStatement()
      val res = statement.executeUpdate(updateString)
      res
    }
    else {
        val query = new Query()
        query.setQuery(updateString)
        val handle = impalaClient.query(query)
        1
/*        val res = impalaClient.fetch(handle,true,100)
        res.getDataSize*/
    }

  }


}
