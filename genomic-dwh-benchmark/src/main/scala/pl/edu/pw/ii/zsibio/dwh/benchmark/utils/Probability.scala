package pl.edu.pw.ii.zsibio.dwh.benchmark.utils

/**
  * @author dawid 
  */
object Probability {

  implicit class ProbabilityChoose[B, R](seq: Iterable[(B, R)])(implicit num: Numeric[B]) {

    lazy val distr = doCountDistribution()

    def countDistribution(): Iterable[(Double, R)] = distr

    private def doCountDistribution(): Iterable[(Double, R)] = {
      val sum = seq.map(x => num.toDouble(x._1)).sum
      val sumOfPrevious = seq.scanLeft(0.0)((p, c) => p + num.toDouble(c._1))
      seq
        .zip(sumOfPrevious)
        .map(x => ((num.toDouble(x._1._1) + x._2) / sum, x._1._2))
    }

    def selectWithProbability(): R = {
      val distr = seq.countDistribution()
      val rand = math.random

      distr.find(rand <= _._1).map(_._2).get
    }

  }

}
