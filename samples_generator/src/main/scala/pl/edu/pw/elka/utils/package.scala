package pl.edu.pw.elka

/**
  * @author dawid 
  */
package object utils {

  case class Range(left: Double, right: Double) {
    def containsRightOpen(x: Double) = left <= x && x < right

    def containsRightClose(x: Double) = left <= x && x <= right
  }

}
