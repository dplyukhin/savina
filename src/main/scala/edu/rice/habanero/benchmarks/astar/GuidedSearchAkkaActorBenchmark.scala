package edu.rice.habanero.benchmarks.astar

import java.util
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.astar.GuidedSearchConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object GuidedSearchAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new GuidedSearchAkkaActorBenchmark)
  }

  private final class GuidedSearchAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      GuidedSearchConfig.parseArgs(args)
    }

    def printArgInfo() {
      GuidedSearchConfig.printArgs()
    }

    private var system: ActorSystem = _
    def runIteration() {

      system = AkkaActorState.newActorSystem("GuidedSearch")

      val latch = new CountDownLatch(1)
      val master = system.actorOf(Props(new Master(latch)))

      latch.await()

      val nodesProcessed = GuidedSearchConfig.nodesProcessed()
      track("Nodes Processed", nodesProcessed)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
      val valid = GuidedSearchConfig.validate()
      printf(BenchmarkRunner.argOutputFormat, "Result valid", valid)
      GuidedSearchConfig.initializeData()
    }
  }

  trait Msg
  case class WorkMessage(node: GuidedSearchConfig.GridNode, target: GuidedSearchConfig.GridNode) extends Msg {
    val priority: Int = GuidedSearchConfig.priority(node)
  }
  case object ReceivedMessage extends Msg
  case object DoneMessage extends Msg
  case object StopMessage extends Msg

  private class Master(latch: CountDownLatch) extends AkkaActor[AnyRef] {

    private final val numWorkers = GuidedSearchConfig.NUM_WORKERS
    private final val workers = new Array[ActorRef](numWorkers)
    private var numWorkersTerminated: Int = 0
    private var numWorkSent: Int = 0
    private var numWorkCompleted: Int = 0

    var i: Int = 0
    while (i < numWorkers) {
      workers(i) = context.actorOf(Props(new Worker(self, i)))
      i += 1
    }
    sendWork(new WorkMessage(originNode, targetNode))

    private def sendWork(workMessage: WorkMessage) {
      val workerIndex: Int = numWorkSent % numWorkers
      numWorkSent += 1
      workers(workerIndex) ! workMessage
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: WorkMessage =>
          sendWork(workMessage)
        case _: ReceivedMessage.type =>
          numWorkCompleted += 1
          if (numWorkCompleted == numWorkSent) {
            latch.countDown()
          }
        case _: DoneMessage.type =>
          latch.countDown()
        case _ =>
      }
    }
  }

  private class Worker(master: ActorRef, id: Int) extends AkkaActor[AnyRef] {

    private final val threshold = GuidedSearchConfig.THRESHOLD

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: WorkMessage =>
          search(workMessage)
          master ! ReceivedMessage
        case _ =>
      }
    }

    private def search(workMessage: WorkMessage) {

      val targetNode = workMessage.target
      val workQueue = new util.LinkedList[GridNode]
      workQueue.add(workMessage.node)

      var nodesProcessed: Int = 0
      while (!workQueue.isEmpty && nodesProcessed < threshold) {

        nodesProcessed += 1
        GuidedSearchConfig.busyWait()

        val loopNode = workQueue.poll
        val numNeighbors: Int = loopNode.numNeighbors

        var i: Int = 0
        while (i < numNeighbors) {
          val loopNeighbor = loopNode.neighbor(i)
          val success: Boolean = loopNeighbor.setParent(loopNode)
          if (success) {
            if (loopNeighbor eq targetNode) {
              master ! DoneMessage
              return
            } else {
              workQueue.add(loopNeighbor)
            }
          }
          i += 1
        }
      }

      while (!workQueue.isEmpty) {
        val loopNode = workQueue.poll
        val newWorkMessage = new WorkMessage(loopNode, targetNode)
        master ! newWorkMessage
      }
    }
  }

}
