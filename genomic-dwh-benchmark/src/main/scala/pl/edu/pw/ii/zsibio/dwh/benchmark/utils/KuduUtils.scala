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
class KuduUtils(kuduMaster:String) {

  val kuduClient = new KuduClientBuilder(kuduMaster).build()
  val log = Logger.getLogger("pl.edu.pw.ii.zsibio.dwh.benchmark.utils.KuduUtils")

  def createTable(ddl:String, tableName: String, dropIfExists:Boolean = false, partitions:Int = 100, replicas:Int = 2) = {

    val columns = new util.ArrayList[ColumnSchema]()
    val schemaSQL = ddl2SparkSQLSchema(ddl)
    schemaSQL.fields
      .foreach {
        f => {
          val col = new ColumnSchemaBuilder(f.name,
            f.dataType match {
              case StringType => Type.STRING
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
    val pkColumns = schemaSQL
      .fields
      .filter(f=>f.nullable == false)
      .map(_.name)
      .toList
      .asJava
    val tabOpts = new CreateTableOptions()
    tabOpts.addHashPartitions(pkColumns,partitions)
    tabOpts.setNumReplicas(replicas)
    val schema = new Schema(columns)
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

  private def ddl2SparkSQLSchema(ddl:String): StructType = {
    val schema = new StructType()
    ddl.split('\n').foreach(l=>log.debug(l))
    schema
  }


}
