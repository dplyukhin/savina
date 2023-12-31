package edu.rice.habanero.benchmarks.apsp

import akka.actor.{ActorRef, ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ListBuffer

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ApspAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ApspAkkaActorBenchmark)
  }

  private final class ApspAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ApspConfig.parseArgs(args)
      ApspUtils.generateGraph()
    }

    def printArgInfo() {
      ApspConfig.printArgs()
    }

    private var system: ActorSystem = _
    def runIteration() {

      val graphData = ApspUtils.graphData
      val numNodes = ApspConfig.N
      val blockSize = ApspConfig.B

      val numBlocksInSingleDim: Int = numNodes / blockSize

      system = AkkaActorState.newActorSystem("ForkJoin")
      val latch = new CountDownLatch(numBlocksInSingleDim * numBlocksInSingleDim)

      // create and automatically the actors
      val blockActors = Array.tabulate[ActorRef](numBlocksInSingleDim, numBlocksInSingleDim) {
        (i, j) =>
          val myBlockId = (i * numBlocksInSingleDim) + j
          val apspActor = system.actorOf(Props(new ApspFloydWarshallActor(myBlockId, blockSize, numNodes, graphData, latch)))
          apspActor
      }
      // create the links to the neighbors
      for (bi <- 0 until numBlocksInSingleDim) {
        for (bj <- 0 until numBlocksInSingleDim) {

          val neighbors = new ListBuffer[ActorRef]()

          // add neighbors in same column
          for (r <- 0 until numBlocksInSingleDim) {
            if (r != bi) {
              neighbors.append(blockActors(r)(bj))
            }
          }
          // add neighbors in same row
          for (c <- 0 until numBlocksInSingleDim) {
            if (c != bj) {
              neighbors.append(blockActors(bi)(c))
            }
          }

          blockActors(bi)(bj) ! ApspNeighborMessage(neighbors)
        }
      }

      // start the computation
      for (bi <- 0 until numBlocksInSingleDim) {
        for (bj <- 0 until numBlocksInSingleDim) {
          blockActors(bi)(bj) ! ApspInitialMessage
        }
      }

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      AkkaActorState.awaitTermination(system)
      ApspUtils.generateGraph()
    }
  }

  sealed abstract class ApspMessage

  private case object ApspInitialMessage extends ApspMessage

  private case class ApspResultMessage(k: Int, myBlockId: Int, initData: Array[Array[Long]]) extends ApspMessage

  private case class ApspNeighborMessage(neighbors: ListBuffer[ActorRef]) extends ApspMessage

  private class ApspFloydWarshallActor(myBlockId: Int, blockSize: Int, graphSize: Int, initGraphData: Array[Array[Long]], latch: CountDownLatch) extends AkkaActor[AnyRef] {

    private val numBlocksInSingleDim: Int = graphSize / blockSize
    private val numNeighbors: Int = 2 * (numBlocksInSingleDim - 1)

    final val rowOffset: Int = (myBlockId / numBlocksInSingleDim) * blockSize
    final val colOffset: Int = (myBlockId % numBlocksInSingleDim) * blockSize

    private val neighbors = new ListBuffer[ActorRef]()

    private var k = -1
    private val neighborDataPerIteration = new java.util.HashMap[Int, Array[Array[Long]]]()

    private var receivedNeighbors = false

    private var currentIterData = ApspUtils.getBlock(initGraphData, myBlockId)

    override def process(msg: AnyRef) {
      msg match {
        case message: ApspResultMessage =>
          if (!receivedNeighbors) {
            val msg = "Block-" + myBlockId + " hasn't received neighbors yet!"
            println("ERROR: " + msg)
            throw new Exception(msg)
          }

          val haveAllData = storeIterationData(message.k, message.myBlockId, message.initData)
          if (haveAllData) {
            // received enough data from neighbors, can proceed to do computation for next k
            k += 1

            performComputation()
            notifyNeighbors()
            neighborDataPerIteration.clear()

            if (k == graphSize - 1) {
              // we've completed the computation
              latch.countDown()
              exit()
            }
          }

        case ApspInitialMessage =>

          notifyNeighbors()

        case ApspNeighborMessage(msgNeighbors) =>

          receivedNeighbors = true
          msgNeighbors.foreach {
            loopNeighbor => neighbors.append(loopNeighbor)
          }
      }
    }

    private def storeIterationData(iteration: Int, sourceId: Int, dataArray: Array[Array[Long]]): Boolean = {
      neighborDataPerIteration.put(sourceId, dataArray)
      neighborDataPerIteration.size() == numNeighbors
    }

    private def performComputation(): Unit = {
      val prevIterData = currentIterData
      // make modifications on a fresh local data array for this iteration
      currentIterData = Array.tabulate[Long](blockSize, blockSize)((i, j) => 0)

      for (i <- 0 until blockSize) {
        for (j <- 0 until blockSize) {
          val gi = rowOffset + i
          val gj = colOffset + j

          val newIterData = elementAt(gi, k, k - 1, prevIterData) + elementAt(k, gj, k - 1, prevIterData)
          currentIterData(i)(j) = scala.math.min(prevIterData(i)(j), newIterData)
        }
      }
    }

    private def elementAt(row: Int, col: Int, srcIter: Int, prevIterData: Array[Array[Long]]): Long = {
      val destBlockId = ((row / blockSize) * numBlocksInSingleDim) + (col / blockSize)
      val localRow = row % blockSize
      val localCol = col % blockSize

      // println("Accessing block-" + destBlockId + " from block-" + selfActor.myBlockId + " for " + (row, col))
      if (destBlockId == myBlockId) {
        prevIterData(localRow)(localCol)
      } else {
        val blockData = neighborDataPerIteration.get(destBlockId)
        blockData(localRow)(localCol)
      }
    }

    private def notifyNeighbors(): Unit = {

      // send the current result to all other blocks who might need it
      // note: this is inefficient version where data is sent to neighbors
      // who might not need it for the current value of k
      val resultMessage = ApspResultMessage(k, myBlockId, currentIterData)
      neighbors.foreach {
        loopNeighbor =>
          loopNeighbor ! resultMessage
      }
    }
  }

}
