package pl.edu.pw.ii.zsibio.dwh.benchmark.utils

/**
  * Created by marek on 14.01.17.
  */
class ExecutionTiming() {

  var startTime:Long = _
  var endTime:Long = _

  def startTimer() = {startTime = System.currentTimeMillis() }
  def stopTimer() = {endTime = System.currentTimeMillis() }
  def getTiming() = endTime - startTime

}
