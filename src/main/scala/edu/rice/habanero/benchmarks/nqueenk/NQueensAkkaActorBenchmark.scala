package edu.rice.habanero.benchmarks.nqueenk

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object NQueensAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new NQueensAkkaActorBenchmark)
  }

  private final class NQueensAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      NQueensConfig.parseArgs(args)
    }

    def printArgInfo() {
      NQueensConfig.printArgs()
    }

    private var system: ActorSystem = _
    def runIteration() {
      val numWorkers: Int = NQueensConfig.NUM_WORKERS
      val priorities: Int = NQueensConfig.PRIORITIES
      val master: Array[ActorRef] = Array(null)

      system = AkkaActorState.newActorSystem("NQueens")
      val latch = new CountDownLatch(1)
      master(0) = system.actorOf(Props(new Master(numWorkers, priorities, latch)))

      latch.await()

      val expSolution = NQueensConfig.SOLUTIONS(NQueensConfig.SIZE - 1)
      val actSolution = Master.resultCounter
      val solutionsLimit = NQueensConfig.SOLUTIONS_LIMIT
      val valid = actSolution >= solutionsLimit && actSolution <= expSolution
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
      Master.resultCounter = 0
    }
  }

  trait Msg
  case class WorkMessage(_priority: Int, data: Array[Int], depth: Int) extends Msg {
    val priority = Math.min(NQueensConfig.PRIORITIES - 1, Math.max(0, _priority))
  }
  case object ResultMessage extends Msg
  case object DoneMessage extends Msg
  case object StopMessage extends Msg

  object Master {
    var resultCounter: Long = 0
  }

  private class Master(numWorkers: Int, priorities: Int, latch: CountDownLatch) extends AkkaActor[AnyRef] {

    private val solutionsLimit = NQueensConfig.SOLUTIONS_LIMIT
    private final val workers = new Array[ActorRef](numWorkers)
    private var messageCounter: Int = 0
    private var numWorkersTerminated: Int = 0
    private var numWorkSent: Int = 0
    private var numWorkCompleted: Int = 0

    var i: Int = 0
    while (i < numWorkers) {
      workers(i) = context.actorOf(Props(new Worker(self, i)))
      i += 1
    }
    val inArray: Array[Int] = new Array[Int](0)
    val workMessage = WorkMessage(priorities, inArray, 0)
    sendWork(workMessage)

    private def sendWork(workMessage: WorkMessage) {
      workers(messageCounter) ! workMessage
      messageCounter = (messageCounter + 1) % numWorkers
      numWorkSent += 1
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: WorkMessage =>
          sendWork(workMessage)
        case ResultMessage =>
          Master.resultCounter += 1
          if (Master.resultCounter == solutionsLimit) {
            latch.countDown()
          }
        case DoneMessage =>
          numWorkCompleted += 1
          if (numWorkCompleted == numWorkSent) {
            latch.countDown()
          }
        case StopMessage =>
          numWorkersTerminated += 1
          if (numWorkersTerminated == numWorkers) {
            exit()
          }
        case _ =>
      }
    }

    def requestWorkersToTerminate() {
      var i: Int = 0
      while (i < numWorkers) {
        workers(i) ! StopMessage
        i += 1
      }
    }
  }

  private class Worker(master: ActorRef, id: Int) extends AkkaActor[AnyRef] {

    private final val threshold: Int = NQueensConfig.THRESHOLD
    private final val size: Int = NQueensConfig.SIZE

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: WorkMessage =>
          nqueensKernelPar(workMessage)
          master ! DoneMessage
        case StopMessage =>
          master ! theMsg
          exit()
        case _ =>
      }
    }

    private[nqueenk] def nqueensKernelPar(workMessage: WorkMessage) {
      val a: Array[Int] = workMessage.data
      val depth: Int = workMessage.depth
      if (size == depth) {
        master ! ResultMessage
      } else if (depth >= threshold) {
        nqueensKernelSeq(a, depth)
      } else {
        val newPriority: Int = workMessage.priority - 1
        val newDepth: Int = depth + 1
        var i: Int = 0
        while (i < size) {
          val b: Array[Int] = new Array[Int](newDepth)
          System.arraycopy(a, 0, b, 0, depth)
          b(depth) = i
          if (NQueensConfig.boardValid(newDepth, b)) {
            master ! WorkMessage(newPriority, b, newDepth)
          }
          i += 1
        }
      }
    }

    private[nqueenk] def nqueensKernelSeq(a: Array[Int], depth: Int) {
      if (size == depth) {
        master ! ResultMessage
      }
      else {
        val b: Array[Int] = new Array[Int](depth + 1)

        var i: Int = 0
        while (i < size) {
          System.arraycopy(a, 0, b, 0, depth)
          b(depth) = i
          if (NQueensConfig.boardValid(depth + 1, b)) {
            nqueensKernelSeq(b, depth + 1)
          }

          i += 1
        }
      }
    }
  }

}
