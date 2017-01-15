package pl.edu.pw.ii.zsibio.dwh.benchmark.utils

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.Calendar

import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.{JDBCConnection, QueryResult}
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import org.apache.log4j.Logger
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.JDBCDriver.Value
import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.QueryType.QueryType
/**
  * Created by marek on 15.01.17.
  */

case class Query(queryId:String, queryType:String, queryEngine:String, queryDesc:String, statement:String)

object QueryType extends Enumeration {

  type QueryType = Value
  val SELECT, CREATE, UPDATE  = Value


}
object QueryExecutorWithLogging {
  val log = Logger.getLogger("pl.edu.pw.ii.zsibio.dwh.benchmark.utils.QueryExecutorWithLogging")
  object QueryYamlProtocol extends DefaultYamlProtocol {
    implicit val queryFormat = yamlFormat5(Query)
  }


  def runStatement(query: Query,conn:JDBCConnection,logFile:String) = {
    log.info(s"Running ${query.queryId} ... using ${query.queryEngine} engine")
    log.debug(s"Executing query: ${query.statement}")
    query.queryType.toLowerCase() match {
      case "select" => logQuery(conn, query, logFile)
      case _ => conn.executeUpdate(query.statement)
    }


  }

  def parseQuertYAML(file:String) : Query ={
    val lines = scala.io.Source.fromFile(file).mkString
    val yml = lines.stripMargin.parseYaml
    import QueryYamlProtocol._
    yml.convertTo[Query]
  }

  private def logQuery(conn:JDBCConnection,query: Query, logFile:String) ={
    val rs = conn.executeQuery(query.statement,true)
    rs.rs.next()
    val result = s"${Calendar.getInstance().getTime().toString},${query.queryId},${query.queryEngine},${rs.timing.get.getTiming()}"
    log.info(s"Result: ${result}")
    val writer = new PrintWriter(new FileOutputStream(new File(logFile),true))
    writer.write(result)
    writer.flush()
    writer.close()


  }

}
