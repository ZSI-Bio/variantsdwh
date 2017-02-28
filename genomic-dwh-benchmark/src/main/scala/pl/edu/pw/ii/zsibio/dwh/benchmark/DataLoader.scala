package pl.edu.pw.ii.zsibio.dwh.benchmark

import com.typesafe.config.ConfigFactory
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
  * Created by marek on 23.01.17.
  */
object DataLoader {
  class RunConf(args:Array[String]) extends ScallopConf(args){

    val csvFile =opt[String]("csvFile",required = true, descr = "A CSV file to load" )
    val tableName =opt[String]("tableName",required = true, descr = "A table to load" )
    val storageType = opt[String]("storageType",required = true, descr = "Storage type parquet|orc|kudu|carbon")
    val dbName =opt[String]("dbName",required = true, descr = "Database name" )


    verify()
  }
  def main(args: Array[String]): Unit = {
    val runConf = new RunConf(args)
    val scConf = new SparkConf()
        .setAppName("DataLoader")
    val sc = new SparkContext(scConf)
    val sqlContext = new HiveContext(sc)


    if(runConf.storageType().toLowerCase() == "orc" || runConf.storageType().toLowerCase() == "parquet") {
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .option("nullValue","\\N")
        .option("inferSchema", "true") // Automatically infer data types
        .load(runConf.csvFile())
        .repartition(10)
      df.registerTempTable("temp_csv")
      sqlContext.sql(
        s"""
        |INSERT OVERWRITE TABLE ${runConf.dbName()}.${runConf.tableName()}

        |SELECT * FROM temp_csv
        """.stripMargin)
      }
    if(runConf.storageType().toLowerCase() == "kudu"){
      val confFile = ConfigFactory.load()
      val kuduMaster = confFile.getString("kudu.master.server")
      val kuduContext = new KuduContext(kuduMaster)
      val dfTarget = sqlContext.read.options(Map("kudu.master" -> kuduMaster,"kudu.table" -> runConf.tableName())).kudu
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .option("nullValue","\\N")
        .schema(dfTarget.schema)
        .load(runConf.csvFile())
        .repartition(10)
      kuduContext.upsertRows(df,runConf.tableName())
    }

  }

  private def synSchemas(inSchema:StructType, outSchema:StructType) = {

    val size = inSchema.fields.length
    val structFields = (0 to size - 1).map{
      i => StructField(outSchema.fields(i).name,inSchema.fields(i).dataType,outSchema.fields(i).nullable)
    }
    new StructType(structFields.toArray)

  }

}
