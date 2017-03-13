package pl.edu.pw.ii.zsibio.dwh.benchmark.generation

import java.lang

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import pl.edu.pw.ii.zsibio.dwh.benchmark.generation.model.GeneratedSample
import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.Probability._
import scala.util.Random

/**
  * @author dawid 
  */
class SamplesGenerator(config: GenerationConfiguration) extends Serializable {


  def generate(sc: SparkContext, n: Int): RDD[GeneratedSample] = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

/*    val population = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(config.dictPath)
      .select($"C4".as("population"), $"C0".as("id"), $"C2".as("countryName"), $"C3".as("region"))
      .orderBy("region", "countryName")*/

    val population = sqlContext.sql(
      s"""
        |SELECT
        |population,
        |geo_id as id,
        |geo_country_name_en as countryName,
        |geo_region_name_en as region
        |FROM
        |${config.countryPopulation}
      """.stripMargin)

    val populationDist = population.map {
      case Row(population: Int, id: Int, countryName: String, region: String) =>
        (region, (population.toLong, id))
    }.groupByKey().collect().toMap

    def selectCountry(region: String): Int = {
      populationDist(region).selectWithProbability()
    }

    sc.parallelize(1 to n)
      .map((_, selectRegion()))
      .map(row => GeneratedSample(row._1, selectCountry(row._2.regionName), selectDisease(), row._2.afColumn))
  }

  def selectDisease() = {
    val r = Math.random()
    val ret: java.lang.Long = if (r <= 0.05)
      (Random.nextInt(60) * 100).toLong
    else
      new lang.Long(-1)
    ret
  }

  def selectRegion(): RegionConfig = {
    val percentages = Seq((config.africaConfig.percent, config.africaConfig)
      , (config.americasConfig.percent, config.americasConfig)
      , (config.europaConfig.percent, config.europaConfig)
      , (config.finnishConfig.percent, config.finnishConfig)
      , (config.southAsianConfig.percent, config.southAsianConfig)
      , (config.westAsianConfig.percent, config.westAsianConfig)
    )

    percentages.selectWithProbability()
  }

}
