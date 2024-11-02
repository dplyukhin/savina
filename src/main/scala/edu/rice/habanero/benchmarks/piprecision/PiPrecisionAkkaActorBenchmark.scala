package edu.rice.habanero.benchmarks.piprecision

import java.math.BigDecimal
import java.util.concurrent.atomic.AtomicInteger

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PiPrecisionAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PiPrecisionAkkaActorBenchmark)
  }

  private final class PiPrecisionAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PiPrecisionConfig.parseArgs(args)
    }

    def printArgInfo() {
      PiPrecisionConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = PiPrecisionConfig.NUM_WORKERS
      val precision: Int = PiPrecisionConfig.PRECISION

      val system = AkkaActorState.newActorSystem("PiPrecision")

      val master = system.actorOf(Props(new Master(numWorkers, precision)))
      AkkaActorState.startActor(master)

      master ! StartMessage

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message

  private case object StartMessage extends Msg with NoRefs
  private case object StopMessage extends Msg with NoRefs
  private case class WorkMessage(scale: Int, term: Int) extends Msg with NoRefs
  private case class ResultMessage(result: BigDecimal, workerId: Int) extends Msg with NoRefs

  private class Master(numWorkers: Int, scale: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val workers = Array.tabulate[ActorRef[Msg]](numWorkers)(i => ctx.spawnAnonymous(Behaviors.setup { ctx => new Worker(self, i, ctx)}))
    private var result: BigDecimal = BigDecimal.ZERO
    private final val tolerance = BigDecimal.ONE.movePointLeft(scale)
    private final val numWorkersTerminated: AtomicInteger = new AtomicInteger(0)
    private var numTermsRequested: Int = 0
    private var numTermsReceived: Int = 0
    private var stopRequests: Boolean = false

    override def onPostStart() {
      workers.foreach(loopWorker => {
        AkkaActorState.startActor(loopWorker)
      })
    }

    /**
     * Generates work for the given worker
     *
     * @param workerId the id of te worker to send work
     */
    private def generateWork(workerId: Int) {
      val wm: PiPrecisionConfig.WorkMessage = new PiPrecisionConfig.WorkMessage(scale, numTermsRequested)
      workers(workerId) ! wm
      numTermsRequested += 1
    }

    def requestWorkersToExit() {
      workers.foreach(loopWorker => {
        loopWorker ! StopMessage
      })
    }

    override def process(msg: Msg) {
      msg match {
        case rm: PiPrecisionConfig.ResultMessage =>
          numTermsReceived += 1
          result = result.add(rm.result)
          if (rm.result.compareTo(tolerance) <= 0) {
            stopRequests = true
          }
          if (!stopRequests) {
            generateWork(rm.workerId)
          }
          if (numTermsReceived == numTermsRequested) {
            requestWorkersToExit()
          }
        case _: PiPrecisionConfig.StopMessage =>
          val numTerminated: Int = numWorkersTerminated.incrementAndGet
          if (numTerminated == numWorkers) {
            exit()
          }
        case _: PiPrecisionConfig.StartMessage =>
          var t: Int = 0
          while (t < Math.min(scale, 10 * numWorkers)) {
            generateWork(t % numWorkers)
            t += 1
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }

    def getResult: String = {
      result.toPlainString
    }
  }

  private class Worker(master: ActorRef[Msg], id: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    override def process(msg: Msg) {
      msg match {
        case _: PiPrecisionConfig.StopMessage =>
          master ! new PiPrecisionConfig.StopMessage
          exit()
        case wm: PiPrecisionConfig.WorkMessage =>
          val result: BigDecimal = PiPrecisionConfig.calculateBbpTerm(wm.scale, wm.term)
          master ! new PiPrecisionConfig.ResultMessage(result, id)
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
