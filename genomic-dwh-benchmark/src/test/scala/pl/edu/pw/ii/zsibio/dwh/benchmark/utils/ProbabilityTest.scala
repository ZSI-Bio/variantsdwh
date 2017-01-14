package pl.edu.pw.ii.zsibio.dwh.benchmark.utils

import org.scalatest.{Matchers, FunSuite}
import pl.edu.pw.elka.utils.Probability._

/**
  * @author dawid 
  */
class ProbabilityTest extends FunSuite with Matchers {

  test("Counting distribution of tuples") {
    val probability = Seq((0.25, 2), (0.3, 4), (0.3, 6), (0.15, 8))

    val distr = probability.countDistribution()

    distr shouldEqual Seq((0.25, 2), (0.25 + 0.3, 4), (0.25 + 0.3 + 0.3, 6), (0.25 + 0.3 + 0.3 + 0.15, 8))
  }

  test("Choose with probability") {
    val probability = Seq((0,"ABC"), (0,"DEF"), (1, "GHI"))

    val results : Seq[String] = (1 to 1000).map(_ => probability.selectWithProbability())

    results.forall(_.equals("GHI")) shouldBe true
  }


  ignore("Choose with probability two with same probability") {
    val probability = Seq((0.0,"ABC"), (0.5,"DEF"), (0.5, "GHI"))

    val results : Seq[String] = (1 to 5000).map(_ => probability.selectWithProbability())

    val firstResult = results.count(_.equals("DEF"))
    val secondResult = results.count(_.equals("GHI"))

    firstResult shouldEqual secondResult +- 100
  }
}
