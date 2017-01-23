package pl.edu.pw.ii.zsibio.dwh.benchmark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

/**
  * Created by marek on 23.01.17.
  */
object DataLoader {
  class RunConf(args:Array[String]) extends ScallopConf(args){

    val csvFile =opt[String]("csvFile",required = true, descr = "A CSV file to load" )
    val tableName =opt[String]("tableName",required = true, descr = "A table to load" )
    val dbName =opt[String]("dbName",required = true, descr = "Database name" )

    verify()
  }
  def main(args: Array[String]): Unit = {
    val runConf = new RunConf(args)
    val scConf = new SparkConf()
        .setAppName("DataLoader")
    val sc = new SparkContext(scConf)
    val sqlContext = new HiveContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "|")
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

}
