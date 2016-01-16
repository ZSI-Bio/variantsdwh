package pl.edu.pw.elka.generation

/**
  * @author dawid 
  */
sealed abstract class RegionConfig(column: String, region: String, per: Double) extends Serializable {
  def afColumn = column

  def regionName = region

  def percent = per
}

case class AmericasConfig(per: Double) extends RegionConfig("EXAC_AMR_AF", "Americas", per)

case class EuropaConfig(per: Double) extends RegionConfig("EXAC_NFE_AF", "Europe", per)

case class FinnishConfig(per: Double) extends RegionConfig("EXAC_FIN_AF", "Europe", per)

case class SouthAsianConfig(per: Double) extends RegionConfig("EXAC_SAS_AF", "Asia", per)

case class WestAsianConfig(per: Double) extends RegionConfig("EXAC_EAS_AF", "Asia", per)

case class AfricaConfig(per: Double) extends RegionConfig("EXAC_AFR_AF", "Africa", per)

case class GenerationConfiguration(dictPath: String
                                   , americasConfig: AmericasConfig = AmericasConfig(16)
                                   , europaConfig: EuropaConfig = EuropaConfig(16)
                                   , finnishConfig: FinnishConfig = FinnishConfig(17)
                                   , southAsianConfig: SouthAsianConfig = SouthAsianConfig(17)
                                   , westAsianConfig: WestAsianConfig = WestAsianConfig(17)
                                   , africaConfig: AfricaConfig = AfricaConfig(17)
                                  ) {
}
