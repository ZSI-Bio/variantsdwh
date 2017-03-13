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

    val annotationsTable = opt[String](required = false, default = Some("ANNOTATIONS"), descr = "Name of a table in " +
      "hive that contains dbNSFP annotations.")
    val countryPopulation = opt[String](required = true, descr = "Name of a table with their population for " +
      "generating samples.")
    val tempTable = opt[String](required = true, descr = "Temp table name where to store generated variants.")
   /* val factTable = opt[String](required = true, descr = "Fact table with all dim ids to store generated variants.")
    val dimVariantPredictions = opt[String](required = true, descr = "dim_variant_predictions table.")
    val dimGenomicPostionEnembl = opt[String](required = true, descr = "dim_genomic_position_ensembl.")
    val dimGenomicPostionRefseq = opt[String](required = true, descr = "dim_genomic_position_refseq.")
  */  val samplesNumber = opt[Int](required = true, descr = "Number of samples to generate.")
    val outputPartitions = opt[Int](required = false, descr = "Number of output partitions.", default = Some(1000))
    val samplesPartitions = opt[Int](required = false, descr = "Number of partitions for samples.", default = Some(141))
    val annotationsPartitions = opt[Int](required = false, descr = "Number of partitions for annotations.", default =
      Some(141))
  }

  def main(args: Array[String]) {
    val params = new Conf(args)
    params.verify()

    val sparkConf = new SparkConf().setAppName("Generate random variants")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val annotations = readAnnotations(sqlContext, params.annotationsTable())

    import sqlContext.implicits._
    new SamplesGenerator(new GenerationConfiguration(params.countryPopulation()))
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

    sqlContext.sql(s"CREATE TABLE ${params.tempTable()} STORED AS ORC AS SELECT * FROM resultTemp")
    sqlContext.sql("DROP TABLE samplesTemp")
    /* load fact_table*/
    /*sqlContext.sql(
      s"""
         |INSERT OVERWRITE TABLE ${params.factTable}
         |SELECT
         |f_sample_id,
         |f_geo_id,
         |f_d_id,
         |f_ensembl_gp_id,
         |f_refseq_gp_id,
         |f_v_id,
         |f_variant_name,
         |'chr'||f_chr,
         |f_pos,
         |f_ref,
         |f_alt,
         |f_alter_depth,
         |f_total_depth,
         |f_genotype,
         |FROM
         |${params.tempTable()} t
         |JOIN
         |${params.dimGenomicPostionEnembl} dge ON ('chr'||t.f_chr=dge.f_chr AND t.f_pos=dge.f_pos)
         |JOIN
         |${params.dimGenomicPostionRefseq} dgr ON ('chr'||t.f_chr=dgr.f_chr AND t.f_pos=dgr.f_pos)
         |JOIN
         |${params.dimVariantPredictions} dv ON (t.f_chr=dv.v_chr AND t.f_pos=dv.v_pos AND t.f_alt = dv.v_alt)
       """.stripMargin)*/

    sc.stop()

  }


  def readAnnotations(sqlContext: HiveContext, annotationsTable: String): RDD[Map[String, Any]] = {
    sqlContext.sql(
      s"""SELECT ref as REFERENCE
              , alt as ALTERNATIVE
              , HG19_CHR
              , `hg19_pos(1-based)` as HG19_POS
              , 10.0 as "MEAN"
              , EXAC_AMR_AF
              , EXAC_NFE_AF
              , EXAC_FIN_AF
              , EXAC_SAS_AF
              , EXAC_EAS_AF
              , EXAC_AFR_AF
        FROM $annotationsTable
        WHERE EXAC_AMR_AF IS NOT NULL AND EXAC_AMR_AF <> '.'
              EXAC_NFE_AF IS NOT NULL AND EXAC_NFE_AF <> '.'
              EXAC_FIN_AF IS NOT NULL AND EXAC_FIN_AF <> '.'
              EXAC_SAS_AF IS NOT NULL AND EXAC_SAS_AF <> '.'
              EXAC_EAS_AF IS NOT NULL AND EXAC_EAS_AF <> '.'
              EXAC_AFR_AF IS NOT NULL AND EXAC_AFR_AF <> '.' """)
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
