package pl.edu.pw.ii.zsibio.dwh.benchmark.utils

/**
  * Created by marek on 16.01.17.
  */
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.ColumnSchema.{ColumnSchemaBuilder, CompressionAlgorithm}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.spark.sql.types._
import java.util

import org.apache.log4j.Logger

import scala.collection.JavaConverters._


/**
  * Created by mwiewior on 02.11.16.
  */

case class IllegalInputParameterException(msg:String)  extends Throwable(msg)
case class TableDef(tableName:String, struct: StructType)

class KuduUtils(kuduMaster:String) {

  val kuduClient = new KuduClientBuilder(kuduMaster).build()
  val log = Logger.getLogger("pl.edu.pw.ii.zsibio.dwh.benchmark.utils.KuduUtils")

  def createTable(ddl:String, storageType:String, dropIfExists:Boolean = false, partitions:Int = 100, replicas:Int = 2) = {
    log.debug(ddl)
    val columns = new util.ArrayList[ColumnSchema]()
    val tableDef = ddl2SparkSQLSchema(ddl)
    tableDef.struct.fields
      .foreach {
        f => {
          val col = new ColumnSchemaBuilder(f.name,
            f.dataType match {
              case StringType => Type.STRING
              case FloatType => Type.FLOAT
              case IntegerType => Type.INT32
              case LongType => Type.INT64
              case DoubleType => Type.DOUBLE
              case BooleanType => Type.BOOL
              case _ => throw IllegalInputParameterException("Unsupported type while creating a Kudu table.")
            })
            .compressionAlgorithm(CompressionAlgorithm.ZLIB)
            .key(f.nullable match {
              case false => true
              case _ => false
            })
            .nullable(f.nullable match {
              case false => false
              case _ => true
            })
            .build()
          columns.add(col)
        }

      }
    val pkColumns = tableDef.struct
      .fields
      .filter(f=>f.nullable == false)
      .map(_.name)
      .toList
      .asJava
    val tabOpts = new CreateTableOptions()
    tabOpts.addHashPartitions(pkColumns,partitions)
    tabOpts.setNumReplicas(replicas)
    val schema = new Schema(columns)
    val tableName = tableDef.tableName
    if ( ! kuduClient.tableExists(tableName) ) {
      kuduClient.createTable(tableName, schema,tabOpts);
    }
    else{
      kuduClient.deleteTable(tableName)
      kuduClient.createTable(tableName, schema,tabOpts);
    }


  }

  def dropTable (name:String) ={
    if(kuduClient.tableExists(name))
      kuduClient.deleteTable(name)
  }

  def closeConnection()={
    kuduClient.close()
  }

  /*private*/ def ddl2SparkSQLSchema(ddl:String): TableDef = {



    val keyColumns = ddl
      .split("kudu.key_columns")(1)
      .split('=')(1)
      .replace("'","")
      .replace(")","")
      .trim.split(',')
    val result = DdlParser.parse(ddl)
    val tableName = {
     val tableMaybeWithDB = result
        .get
        .name
        .toLowerCase
        .split('.')
      if(tableMaybeWithDB.length == 2)
        tableMaybeWithDB(1)
      else
        tableMaybeWithDB(0)
    }

    val columns = result
      .get
      .columns
      .map{
        c =>
          val splitColumn = c.replace("`","").split(' ').map(v=>v.trim.toLowerCase)
          StructField(splitColumn(0),
            splitColumn(1).toLowerCase match {
              case "string" => StringType
              case "double" => DoubleType
              case "float" => FloatType
              case "int" => IntegerType
              case "bigint" => LongType
              case "boolean" => BooleanType
              case _ => throw new Exception(s"Unsupported data type ${splitColumn(1).toUpperCase()}")
            }, if(keyColumns.contains(splitColumn(0) ) ) false else true
          )
      }
    log.debug(s"Key columns: ${keyColumns.mkString("|")}" )
    log.debug(s"Schema: ${columns.mkString("|")}")
    log.debug(s"Table name: ${tableName}")
    val schema = new StructType(columns.toArray)
    new TableDef(tableName, schema)
  }


}
