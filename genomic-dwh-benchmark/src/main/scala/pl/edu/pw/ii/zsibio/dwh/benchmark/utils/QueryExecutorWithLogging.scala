package pl.edu.pw.ii.zsibio.dwh.benchmark.utils

import pl.edu.pw.ii.zsibio.dwh.benchmark.dao.JDBCConnection
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
/**
  * Created by marek on 15.01.17.
  */

case class Query(queryId:String,queryType:String, queryDesc:String,query:String)
object QueryExecutorWithLogging {

  object QueryYamlProtocol extends DefaultYamlProtocol {
    implicit val queryFormat = yamlFormat4(Query)
  }


  def runStatement(file:String,conn:JDBCConnection) = {
    val query = parseQuertYAML(file)

  }

  private def parseQuertYAML(file:String) : Query ={
    val lines = scala.io.Source.fromFile(file).mkString
    val yml = lines.stripMargin.parseYaml
    import QueryYamlProtocol._
    yml.convertTo[Query]
  }

}
