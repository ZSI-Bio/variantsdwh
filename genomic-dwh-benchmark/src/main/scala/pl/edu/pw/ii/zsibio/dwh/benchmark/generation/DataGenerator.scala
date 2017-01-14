package pl.edu.pw.ii.zsibio.dwh.benchmark.generation

import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.rdd.RDD
import pl.edu.pw.ii.zsibio.dwh.benchmark.generation.model.{GeneratedSample, GeneratedVariant}

/**
  * @author dawid
  * @since  23.05.15.
  */
object DataGenerator {

  /**
    * Represents a variant as map of columns
    */
  type VariantRow = Map[String, Any]

  /**
    * Generates random samples of variants.
    * The generation algorithms is:<br/>
    * for every annotations a genotype is randomized based on the
    * allelic frequency(af) "EXAC_AF" column. The probabilities are like:
    *
    * <table border="1">
    * <tr><td>Genotype</td><td>Probability</td></tr>
    * <tr><td>0/0</td><td>1 - 2 * af + af ^ 2 ^</td></tr>
    * <tr><td>0/1</td><td>2 * af * (1 - af)</td></tr>
    * <tr><td>1/1</td><td>af ^ 2 ^</td></tr>
    * </table>
    *
    * Where the genotypes marked with '*' are considered false positives.
    * <p>
    * Then based on the genotype from the variants set the quality parameters are drawn.
    *
    * @param annotations rdd of annotations providing the snp variations
    * @return plain rdd of samples where each entry is a variant
    */
  def generate(samples: RDD[GeneratedSample],
               annotations: RDD[VariantRow]): RDD[GeneratedVariant] = {

    val result = annotations.cartesian(samples)
      .map(
        x => randomizeGenotype(x._1, x._2.exac_column)
          + (("SAMPLE", x._2.sample))
          + (("COUNTRY", x._2.country))
          + (("DISEASE", x._2.disease))
      )
      .filter(v => v("GENOTYPE") != "0/0")
      .map(randomizeDepths)
      .map(mapToVariant)

    result
  }

  def mapToVariant(row: VariantRow): GeneratedVariant = {
    GeneratedVariant(
      row("SAMPLE").asInstanceOf[Long]
      , s"chr${row("HG19_CHR").asInstanceOf[String]}:${row("HG19_POS").asInstanceOf[Int]}_${row("REFERENCE")
        .asInstanceOf[String]}/${row("ALTERNATIVE").asInstanceOf[String]}"
      , row("COUNTRY").asInstanceOf[java.lang.Long]
      , row("DISEASE").asInstanceOf[Long]
      , s"chr${row("HG19_CHR").asInstanceOf[String]}"
      , row("HG19_POS").asInstanceOf[Int]
      , row("REFERENCE").asInstanceOf[String]
      , row("ALTERNATIVE").asInstanceOf[String]
      , row("ALTERNATIVE_DEPTH").asInstanceOf[Double].toInt
      , row("TOTAL_DEPTH").asInstanceOf[Double].toInt
      , row("GENOTYPE").asInstanceOf[String].replace("*", "")
    )
  }

  /**
    * Draws a genotype as described in DataGenerator.generate()
    * @param newVariant annotation to draw a genotype for
    * @return a variant with drawn genotype
    */
  def randomizeGenotype(newVariant: VariantRow, afColumn: String): VariantRow = {

    implicit class PowerFloat(base: Double) {
      def **(exp: Int): Double = {
        math.pow(base, exp)
      }
    }

    import pl.edu.pw.ii.zsibio.dwh.benchmark.utils.Probability._
    val af = newVariant(afColumn).asInstanceOf[Double]
    val genotypes = Seq(
      (af ** 2, "1/1")
      , (2 * af * (1 - af), "0/1")
      , (1 + (af ** 2) - 2 * af, "0/0")
    )

    val genotype = genotypes.selectWithProbability()

    newVariant + (("GENOTYPE", genotype))
  }


  def randomizeDepths(newVariant: VariantRow): VariantRow = {
    val mean_var = newVariant("MEAN").asInstanceOf[Double]
    val total_depth = math.abs(mean_var + (mean_var * (math.random - 0.5)))

    val allele_depth = newVariant("GENOTYPE").asInstanceOf[String] match {
      case "0/1" => math.abs((total_depth / 2) + 0.2 * ((total_depth / 2) * (math.random - 0.5)))
      case "1/1" => math.abs(total_depth - 0.1 * (total_depth * math.random))
    }

    newVariant + (("TOTAL_DEPTH", total_depth)) + (("ALTERNATIVE_DEPTH", allele_depth))

  }


}
