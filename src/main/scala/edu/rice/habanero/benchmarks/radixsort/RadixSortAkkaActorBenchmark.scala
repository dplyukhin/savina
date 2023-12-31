package edu.rice.habanero.benchmarks.radixsort

import akka.actor.{ActorRef, ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import java.util.concurrent.CountDownLatch

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object RadixSortAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new RadixSortAkkaActorBenchmark)
  }

  private final class RadixSortAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      RadixSortConfig.parseArgs(args)
    }

    def printArgInfo() {
      RadixSortConfig.printArgs()
    }

    private var system: ActorSystem = _
    def runIteration() {

      system = AkkaActorState.newActorSystem("RadixSort")
      val latch = new CountDownLatch(1)

      val validationActor = system.actorOf(Props(new ValidationActor(RadixSortConfig.N, latch)))

      val sourceActor = system.actorOf(Props(new IntSourceActor(RadixSortConfig.N, RadixSortConfig.M, RadixSortConfig.S)))

      var radix = RadixSortConfig.M / 2
      var nextActor: ActorRef = validationActor
      while (radix > 0) {

        val localRadix = radix
        val localNextActor = nextActor
        val sortActor = system.actorOf(Props(new SortActor(RadixSortConfig.N, localRadix, localNextActor)))

        radix /= 2
        nextActor = sortActor
      }

      sourceActor ! NextActorMessage(nextActor)

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private case class NextActorMessage(actor: ActorRef)

  private case class ValueMessage(value: Long)

  private class IntSourceActor(numValues: Int, maxValue: Long, seed: Long) extends AkkaActor[AnyRef] {

    val random = new PseudoRandom(seed)

    override def process(msg: AnyRef) {

      msg match {
        case nm: NextActorMessage =>

          var i = 0
          while (i < numValues) {

            val candidate = Math.abs(random.nextLong()) % maxValue
            val message = new ValueMessage(candidate)
            nm.actor ! message

            i += 1
          }

          exit()
      }
    }
  }

  private class SortActor(numValues: Int, radix: Long, nextActor: ActorRef) extends AkkaActor[AnyRef] {

    private val orderingArray = Array.ofDim[ValueMessage](numValues)
    private var valuesSoFar = 0
    private var j = 0

    override def process(msg: AnyRef): Unit = {
      msg match {
        case vm: ValueMessage =>

          valuesSoFar += 1

          val current = vm.value
          if ((current & radix) == 0) {
            nextActor ! vm
          } else {
            orderingArray(j) = vm
            j += 1
          }

          if (valuesSoFar == numValues) {

            var i = 0
            while (i < j) {
              nextActor ! orderingArray(i)
              i += 1
            }

            exit()
          }
      }
    }
  }

  private class ValidationActor(numValues: Int, latch: CountDownLatch) extends AkkaActor[AnyRef] {

    private var sumSoFar = 0.0
    private var valuesSoFar = 0
    private var prevValue = 0L
    private var errorValue = (-1L, -1)

    override def process(msg: AnyRef) {

      msg match {
        case vm: ValueMessage =>

          valuesSoFar += 1

          if (vm.value < prevValue && errorValue._1 < 0) {
            errorValue = (vm.value, valuesSoFar - 1)
          }
          prevValue = vm.value
          sumSoFar += prevValue

          if (valuesSoFar == numValues) {
            if (errorValue._1 >= 0) {
              println("ERROR: Value out of place: " + errorValue._1 + " at index " + errorValue._2)
            } else {
              println("Elements sum: " + sumSoFar)
            }
            latch.countDown()
            exit()
          }
      }
    }
  }

}
