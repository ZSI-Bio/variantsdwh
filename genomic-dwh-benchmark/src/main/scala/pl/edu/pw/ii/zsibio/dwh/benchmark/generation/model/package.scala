package pl.edu.pw.ii.zsibio.dwh.benchmark.generation

/**
  * @author dawid 
  */
package object model {

  case class GeneratedVariant(f_sample_id: Long
                              , f_variant_name: String
                              , f_geo_id: Long
                              , f_d_id: java.lang.Long
                              , f_chr: String
                              , f_pos: Long
                              , f_ref: String
                              , f_alt: String
                              , f_alter_depth: Int
                              , f_total_depth: Int
                              , f_genotype: String
                             )

  case class GeneratedSample(sample: Long
                             , country: Long
                             , disease: java.lang.Long
                             , exac_column: String)

}
