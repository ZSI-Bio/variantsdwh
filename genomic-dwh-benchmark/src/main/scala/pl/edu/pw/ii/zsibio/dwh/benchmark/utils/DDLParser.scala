package pl.edu.pw.ii.zsibio.dwh.benchmark.utils

/**
  * Created by marek on 17.01.17.
  */
import org.parboiled2._
import scala.util.{Failure, Success}


case class tbl(name: String, columns: Seq[String])

/**
  * Shows how easy it is to express a grammar in pseudo-BNF using Parboiled2.
  * @param input
  * @param columnStart
  */
case class DdlParser(input: ParserInput) extends Parser {
  import DdlParser._

  def DDL           = rule { Statements }
  def Statements    = rule { Ignore ~ Table  }
  def Table         = rule { TableFlag ~ TableName ~ Ignore ~ Arguments ~> tbl }
  def TableName     = rule { capture(!EndName ~ ANY).+ ~> (_.mkString("")) ~ EndName}
  def Arguments     = rule { Arg.*.separatedBy((! (oneOrMore(Space))  ~ ANY).+) }
  def Arg           = rule { zeroOrMore(Space) ~ capture(!(Comma | ")" | NewLine) ~ ANY).+  ~>(_.mkString("")) ~(Comma | ")" | NewLine)}
  def TableFlag     = rule { ignoreCase(CreateTable) ~ Space }
  def EndName       = rule { Space | "(" }
  def Ignore        = rule { (! (ignoreCase(CreateTable)| Comma | Space ~ Space)  ~ ANY).+ }
}

object DdlParser {
  val NewLine = "\n"
  val Comma = ","
  val Space = " "
  val CreateTable = "table"

  private val sample =
    """
CREATE EXTERNAL TABLE `dws.test2`(
  a INT,
  b VARCHAR,
  c FLOAT
  ) TBLPROPERTIES(ds)
    """//.stripMargin

  def test = DdlParser(sample).DDL.run()

  def parse(input:String) = {
    DdlParser(input).DDL.run() match {
      case Success(result) => Some(result.asInstanceOf[tbl])
      case _ => None
    }
  }

  def main(args: Array[String]) {
    println(test)
  }
}