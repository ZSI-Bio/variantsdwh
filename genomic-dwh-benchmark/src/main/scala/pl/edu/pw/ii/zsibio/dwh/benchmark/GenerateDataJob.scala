package pl.edu.pw.ii.zsibio.dwh.benchmark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._
import pl.edu.pw.ii.zsibio.dwh.benchmark.generation.model.GeneratedSample
import pl.edu.pw.ii.zsibio.dwh.benchmark.generation.{DataGenerator, GenerationConfiguration, SamplesGenerator}

/**
  * @author dawid
  * @since 08.06.15.
  */
object GenerateDataJob {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

    banner("Usage: spark-submit <spark-options> pl.edu.pw.elka.GenerateDataJob -a [AnnotationTable]" +
      " -d <CountryDictionaryPath> -s <SamplesNumber> -o <OutputPath>")

    val annotationsTable = opt[String](required = false, default = Some("ANNOTATIONS"), descr = "Name of table in " +
      "hive that contains dbNSFP annotations.")
    val dictPath = opt[String](required = true, descr = "Path to dictionary of countries with their population for " +
      "generating samples.")
    val outputTable = opt[String](required = true, descr = "Table name where to store generated variants.")
    val samplesNumber = opt[Int](required = true, descr = "Number of samples to generate.")
    val outputPartitions = opt[Int](required = false, descr = "Number of output partitions.", default = Some(1000))
    val samplesPartitions = opt[Int](required = false, descr = "Number of partitions for samples.", default = Some(141))
    val annotationsPartitions = opt[Int](required = false, descr = "Number of partitions for annotations.", default =
      Some(141))
  }

  def main(args: Array[String]) {
    val params = new Conf(args)

    val sparkConf = new SparkConf().setAppName("Generate random variants")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val annotations = readAnnotations(sqlContext, params.annotationsTable())

    import sqlContext.implicits._
    new SamplesGenerator(new GenerationConfiguration(params.dictPath()))
      .generate(sc, params.samplesNumber()).toDF().registerTempTable("samples")

    sqlContext.sql("CREATE TABLE samplesTemp STORED AS ORC AS SELECT * FROM samples")
    val samples = sqlContext.sql("SELECT * FROM samplesTemp").rdd.map(row => new GeneratedSample(
      row.getLong(0)
      , row.getLong(1)
      , if (row.getLong(2) == -1) null.asInstanceOf[java.lang.Long] else row.getLong(2)
      , row.getString(3)))

    val annCached = annotations.repartition(params.annotationsPartitions())

    val samCached = samples.cache()
    annCached.count()
    samCached.count()

    val m = DataGenerator.generate(samCached
      , annCached)
      .repartition(params.outputPartitions())

    m.toDF().registerTempTable("resultTemp")

    sqlContext.sql(s"CREATE TABLE ${params.outputTable()} STORED AS ORC AS SELECT * FROM resultTemp")
    sqlContext.sql("DROP TABLE samplesTemp")
    sc.stop()

  }


  def readAnnotations(sqlContext: HiveContext, annotationsTable: String): RDD[Map[String, Any]] = {
    sqlContext.sql(
      s"""SELECT REFERENCE
              , ALTERNATIVE
              , HG19_CHR
              , HG19_POS
              , MEAN
              , EXAC_AMR_AF
              , EXAC_NFE_AF
              , EXAC_FIN_AF
              , EXAC_SAS_AF
              , EXAC_EAS_AF
              , EXAC_AFR_AF
        FROM $annotationsTable
        WHERE EXAC_AMR_AF IS NOT NULL AND
              EXAC_NFE_AF IS NOT NULL AND
              EXAC_FIN_AF IS NOT NULL AND
              EXAC_SAS_AF IS NOT NULL AND
              EXAC_EAS_AF IS NOT NULL AND
              EXAC_AFR_AF IS NOT NULL""")
      .map(_.getValuesMap[Any](Seq("REFERENCE"
        , "ALTERNATIVE"
        , "HG19_CHR"
        , "HG19_POS"
        , "MEAN"
        , "EXAC_AMR_AF"
        , "EXAC_NFE_AF"
        , "EXAC_FIN_AF"
        , "EXAC_SAS_AF"
        , "EXAC_EAS_AF"
        , "EXAC_AFR_AF")))
  }

}
