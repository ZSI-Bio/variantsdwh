package pl.edu.pw.ii.zsibio.dwh.benchmark.utils

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.Calendar

import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.{ConnectDriver, EngineConnection, QueryResult}
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import org.apache.log4j.Logger
import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.ConnectDriver.Value
import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.QueryType.QueryType
/**
  * Created by marek on 15.01.17.
  */

case class Query(queryId:String, queryType:String, queryEngine:String, storageFormat:String,queryDesc:String,
                 statement:String, dryRun:Boolean = false)

object QueryType extends Enumeration {

  type QueryType = Value
  val SELECT, CREATE, UPDATE  = Value


}
object QueryExecutorWithLogging {
  val log = Logger.getLogger("pl.edu.pw.ii.zsibio.dwh.benchmark.utils.QueryExecutorWithLogging")
  object QueryYamlProtocol extends DefaultYamlProtocol {
    implicit val queryFormat = yamlFormat7(Query)
  }


  def runStatement(query: Query, conn:EngineConnection, logFile:String) = {
    log.info(s"Running ${query.queryId} ... using ${query.queryEngine} engine")
    log.debug(s"Executing query: ${query.statement}")
    query.queryType.toLowerCase() match {
      case "select" => logQuery(conn, query, logFile)
      case _ => conn.executeUpdate(query.statement.toLowerCase)
    }


  }

  def parseQueryYAML(file:String,storageType:String,connString:String, kuduMaster:String, dbName:String, ifExplain:Boolean = false)  : Query ={
    log.info(s"Parsing ${file}")
    val lines = scala.io.Source.fromFile(file).mkString
    val yml = lines.stripMargin.parseYaml
    import QueryYamlProtocol._
    queryPreprocess(yml.convertTo[Query], storageType, connString, kuduMaster, dbName, ifExplain)

  }

  private def logQuery(conn:EngineConnection, query: Query, logFile:String) ={
    val rs = conn.executeQuery(query.statement.toLowerCase,true)
    rs.rs.next()
    val result = s"${Calendar.getInstance().getTime().toString},${query.queryId}," +
      s"${query.queryEngine},${query.storageFormat},${rs.timing.get.getTiming()}, ${query.dryRun.toString}\n"
    log.info(s"Result: ${result}")
    val writer = new PrintWriter(new FileOutputStream(new File(logFile),true))
    writer.write(result)
    writer.flush()
    writer.close()
  }

  private def queryPreprocess(query: Query, storageType: String, connString: String, kuduMaster: String, dbName: String, ifExplain: Boolean) = {
    def replaceVars(property:String) ={
      property
        .replaceAll("\\{\\{DATA_FORMAT\\}\\}",storageType.toLowerCase)
        .replaceAll("\\{\\{DB_NAME\\}\\}",dbName.toLowerCase)
        .replaceAll("\\{\\{KUDU_MASTER\\}\\}",kuduMaster )
        .replaceAll("\\{\\{IF_EXPLAIN\\}\\}", if(ifExplain) "EXPLAIN " else "")
        .replaceAll("\\{\\{PERCENTILE_APPROX\\}\\}", if(query.queryEngine.toLowerCase=="presto") "approx_percentile" else "percentile_approx")

    }
    query.copy(
      queryId = replaceVars(query.queryId),
      queryDesc = replaceVars(query.queryDesc),
      storageFormat = replaceVars(query.storageFormat),
      statement = replaceVars(query.statement.replaceAll(",",",\n").replaceAll("\\(","\\(  ")),
      dryRun = ifExplain
    )
  }

}
