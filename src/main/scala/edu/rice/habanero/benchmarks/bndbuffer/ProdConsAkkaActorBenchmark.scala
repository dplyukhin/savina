package edu.rice.habanero.benchmarks.bndbuffer

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.bndbuffer.ProdConsBoundedBufferConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.collection.mutable.ListBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ProdConsAkkaActorBenchmark {
  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ProdConsAkkaActorBenchmark)
  }

  private final class ProdConsAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ProdConsBoundedBufferConfig.parseArgs(args)
    }

    def printArgInfo() {
      ProdConsBoundedBufferConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("ProdCons")

      val manager = system.actorOf(Props(
        new ManagerActor(
          ProdConsBoundedBufferConfig.bufferSize,
          ProdConsBoundedBufferConfig.numProducers,
          ProdConsBoundedBufferConfig.numConsumers,
          ProdConsBoundedBufferConfig.numItemsPerProducer)),
        name = "manager")

      AkkaActorState.startActor(manager)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }

    private trait Msg extends Message
    private case class DataItemMessage(d: Double, ref: ActorRef[Msg]) extends Msg {
      override def refs: Iterable[ActorRef[_]] = Some(ref)
    }
    private case object ProduceDataMessage extends Msg with NoRefs
    private case object ProducerExitMessage extends Msg with NoRefs
    private case class ConsumerAvailableMessage(ref: ActorRef[Msg]) extends Msg {
      override def refs: Iterable[ActorRef[_]] = Some(ref)
    }
    private case object ConsumerExitMessage extends Msg with NoRefs


    private class ManagerActor(bufferSize: Int, numProducers: Int, numConsumers: Int, numItemsPerProducer: Int,
                               ctx: ActorContext[Msg], ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {


      private val adjustedBufferSize: Int = bufferSize - numProducers
      private val availableProducers = new ListBuffer[ActorRef[Msg]]
      private val availableConsumers = new ListBuffer[ActorRef[Msg]]
      private val pendingData = new ListBuffer[ProdConsBoundedBufferConfig.DataItemMessage]
      private var numTerminatedProducers: Int = 0

      private val producers = Array.tabulate[ActorRef[Msg]](numProducers)(i =>
        ctx.spawnAnonymous(Behaviors.setup { ctx => new ProducerActor(i, self, numItemsPerProducer, ctx)}))
      private val consumers = Array.tabulate[ActorRef[Msg]](numConsumers)(i =>
        ctx.spawnAnonymous(Behaviors.setup { ctx => new ConsumerActor(i, self, ctx)}))

      override def onPostStart() {
        consumers.foreach(loopConsumer => {
          availableConsumers.append(loopConsumer)
          AkkaActorState.startActor(loopConsumer)
        })

        producers.foreach(loopProducer => {
          AkkaActorState.startActor(loopProducer)
        })

        producers.foreach(loopProducer => {
          loopProducer ! ProduceDataMessage
        })
      }

      override def onPreExit() {
        consumers.foreach(loopConsumer => {
          loopConsumer ! ConsumerExitMessage
        })
      }

      override def process(theMsg: Msg) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            val producer: ActorRef[Msg] = dm.producer.asInstanceOf[ActorRef[Msg]]
            if (availableConsumers.isEmpty) {
              pendingData.append(dm)
            } else {
              availableConsumers.remove(0) ! dm
            }
            if (pendingData.size >= adjustedBufferSize) {
              availableProducers.append(producer)
            } else {
              producer ! ProduceDataMessage
            }
          case cm: ProdConsBoundedBufferConfig.ConsumerAvailableMessage =>
            val consumer: ActorRef[Msg] = cm.consumer.asInstanceOf[ActorRef[Msg]]
            if (pendingData.isEmpty) {
              availableConsumers.append(consumer)
              tryExit()
            } else {
              consumer ! pendingData.remove(0)
              if (!availableProducers.isEmpty) {
                availableProducers.remove(0) ! ProduceDataMessage
              }
            }
          case _: ProdConsBoundedBufferConfig.ProducerExitMessage =>
            numTerminatedProducers += 1
            tryExit()
          case msg =>
            val ex = new IllegalArgumentException("Unsupported message: " + msg)
            ex.printStackTrace(System.err)
        }
      }

      def tryExit() {
        if (numTerminatedProducers == numProducers && availableConsumers.size == numConsumers) {
          exit()
        }
      }
    }

    private class ProducerActor(id: Int, manager: ActorRef[Msg], numItemsToProduce: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

      private var prodItem: Double = 0.0
      private var itemsProduced: Int = 0

      private def produceData() {
        prodItem = processItem(prodItem, prodCost)
        manager ! new ProdConsBoundedBufferConfig.DataItemMessage(prodItem, self)
        itemsProduced += 1
      }

      override def process(theMsg: Msg) {
        if (theMsg.isInstanceOf[ProdConsBoundedBufferConfig.ProduceDataMessage]) {
          if (itemsProduced == numItemsToProduce) {
            exit()
          } else {
            produceData()
          }
        } else {
          val ex = new IllegalArgumentException("Unsupported message: " + theMsg)
          ex.printStackTrace(System.err)
        }
      }

      override def onPreExit() {
        manager ! ProducerExitMessage
      }
    }

    private class ConsumerActor(id: Int, manager: ActorRef[Msg], ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

      private val consumerAvailableMessage = new ProdConsBoundedBufferConfig.ConsumerAvailableMessage(self)
      private var consItem: Double = 0

      protected def consumeDataItem(dataToConsume: Double) {
        consItem = processItem(consItem + dataToConsume, consCost)
      }

      override def process(theMsg: Msg) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            consumeDataItem(dm.data)
            manager ! consumerAvailableMessage
          case _: ProdConsBoundedBufferConfig.ConsumerExitMessage =>
            exit()
          case msg =>
            val ex = new IllegalArgumentException("Unsupported message: " + msg)
            ex.printStackTrace(System.err)
        }
      }
    }

  }

}
