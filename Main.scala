import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RecursiveTask
import java.util.concurrent.ForkJoinWorkerThread
import scala.util.Random
import java.util.concurrent._

import org.scalameter._

object Main {
  val forkJoinPool = new ForkJoinPool

  def task[T](computation: => T): RecursiveTask[T] = {
    val t = new RecursiveTask[T] {
      def compute = computation
    }

    Thread.currentThread match {
      case wt: ForkJoinWorkerThread =>
        t.fork() // schedule for execution
      case _ =>
        forkJoinPool.execute(t)
    }

    t
  }

  def parallel[A, B](taskA: => A, taskB: => B): (A, B) = {
    val right = task { taskB }
    val left = taskA

    (left, right.join())
  }

  def parallel[A, B, C, D](taskA: => A, taskB: => B, taskC: => C, taskD: => D): (A, B, C, D) = {
    val ta = task { taskA }
    val tb = task { taskB }
    val tc = task { taskC }
    val td = taskD
    (ta.join(), tb.join(), tc.join(), td)
  }
  
  //////////////////
  
  def main(args: Array[String]) = {
    def integralSeq(f: (Double) => Double, a: Double, b: Double, numPoints: Int) =
      1.0 / numPoints * (b - a) * sumValues(f, a, b, numPoints)
      
    def integralPar(f: (Double) => Double, a: Double, b: Double, numPoints: Int) = {
      val (sum1, sum2) = parallel(
          sumValues(f, a, b - (b - a) / 2, numPoints / 2),
          sumValues(f, a + (b - a) / 2, b, numPoints / 2))
      1.0 / numPoints * (b - a) * (sum1 + sum2)
    }
    
    def sumValues(f: (Double) => Double, a: Double, b: Double, numPoints: Int): Double = {
      val rand = new Random
      
      def iter(sum: Double, pointsGenerated: Int): Double = {
        if (pointsGenerated == numPoints)
          sum
        else {
          val x = rand.nextDouble() * (b - a) + a
          iter(sum + f(x), pointsGenerated + 1)
        }
      }
      iter(0, 0)
    }
    
    //////////////////
    
    val standardConfig = config(
      Key.exec.minWarmupRuns -> 100,
      Key.exec.maxWarmupRuns -> 10000,
      Key.exec.benchRuns -> 100,
      Key.verbose -> true
    ) withWarmer(new Warmer.Default)
    
    val f = (x: Double) => x
    val a = 0
    val b = 1
    val numPoints = 100000
    
    val seqtime = standardConfig measure {
      val integral = integralSeq(f, a, b, numPoints)
      println(s"Sequential integral value: $integral")
    }
    
    val partime = standardConfig measure {
      val integral = integralPar(f, a, b, numPoints)
      println(s"Parallel integral value: $integral")
    }
    
    println(s"Sequential time: $seqtime")
    println(s"Parallel time: $partime")
    val speedup = seqtime.value / partime.value
    println(s"speedup = $speedup")
  }
}
