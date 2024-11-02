package edu.rice.habanero.benchmarks.trapezoid

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object TrapezoidalAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new TrapezoidalAkkaActorBenchmark)
  }

  private final class TrapezoidalAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      TrapezoidalConfig.parseArgs(args)
    }

    def printArgInfo() {
      TrapezoidalConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("Trapezoidal")

      val numWorkers: Int = TrapezoidalConfig.W
      val precision: Double = (TrapezoidalConfig.R - TrapezoidalConfig.L) / TrapezoidalConfig.N

      val master = system.actorOf(Props(new Master(numWorkers)))
      master ! new WorkMessage(TrapezoidalConfig.L, TrapezoidalConfig.R, precision)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message
  private case class WorkMessage(l: Double, r: Double, h: Double) extends Msg with NoRefs
  private case class ResultMessage(result: Double, workerId: Int) extends Msg with NoRefs

  private class Master(numWorkers: Int) extends GCActor[Msg](ctx) {

    private final val workers = Array.tabulate[ActorRef[Msg]](numWorkers)(i =>
      ctx.spawnAnonymous(Behaviors.setup { ctx => new Worker(self, i, ctx)}))
    private var numTermsReceived: Int = 0
    private var resultArea: Double = 0.0

    override def onPostStart() {
    }

    override def process(msg: Msg) {
      msg match {
        case rm: ResultMessage =>

          numTermsReceived += 1
          resultArea += rm.result

          if (numTermsReceived == numWorkers) {
            println("  Area: " + resultArea)
            exit()
          }

        case wm: WorkMessage =>

          val workerRange: Double = (wm.r - wm.l) / numWorkers
          for ((loopWorker, i) <- workers.view.zipWithIndex) {

            val wl = (workerRange * i) + wm.l
            val wr = wl + workerRange

            loopWorker ! new WorkMessage(wl, wr, wm.h)
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class Worker(master: ActorRef[Msg], val id: Int) extends GCActor[Msg](ctx) {

    override def process(msg: Msg) {
      msg match {
        case wm: WorkMessage =>

          val n = ((wm.r - wm.l) / wm.h).asInstanceOf[Int]
          var accumArea = 0.0

          var i = 0
          while (i < n) {
            val lx = (i * wm.h) + wm.l
            val rx = lx + wm.h

            val ly = TrapezoidalConfig.fx(lx)
            val ry = TrapezoidalConfig.fx(rx)

            val area = 0.5 * (ly + ry) * wm.h
            accumArea += area

            i += 1
          }

          master ! new ResultMessage(accumArea, id)
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
