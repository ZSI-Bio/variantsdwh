package pl.edu.pw.ii.zsibio.dwh.benchmark.utils

/**
  * Created by marek on 14.01.17.
  */
class ExecutionTiming() {

  var startTime:Long = _
  var endTime:Long = _

  def startTimer() = {startTime = System.nanoTime() }
  def stopTimer() = {endTime = System.nanoTime() }
  def getTiming() = Math.round((endTime - startTime)/Math.pow(10,6) )
}
