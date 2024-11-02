package edu.rice.habanero.benchmarks.filterbank

import java.util
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.Collection
import scala.collection.JavaConversions._

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FilterBankAkkaActorBenchmark {

  def main(args: Array[String]) {
    println("WARNING: AKKA version DEADLOCKS. Needs debugging!")
    BenchmarkRunner.runBenchmark(args, new FilterBankAkkaActorBenchmark)
  }

  private final class FilterBankAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FilterBankConfig.parseArgs(args)
    }

    def printArgInfo() {
      FilterBankConfig.printArgs()
    }

    def runIteration() {
      val numSimulations: Int = FilterBankConfig.NUM_SIMULATIONS
      val numChannels: Int = FilterBankConfig.NUM_CHANNELS
      val numColumns: Int = FilterBankConfig.NUM_COLUMNS
      val H: Array[Array[Double]] = FilterBankConfig.H
      val F: Array[Array[Double]] = FilterBankConfig.F
      val sinkPrintRate: Int = FilterBankConfig.SINK_PRINT_RATE

      val system = AkkaActorState.newActorSystem("FilterBank")

      // create the pipeline of actors
      val producer = system.actorOf(Props(new ProducerActor(numSimulations)))
      val sink = system.actorOf(Props(new SinkActor(sinkPrintRate)))
      val combine = system.actorOf(Props(new CombineActor(sink)))
      val integrator = system.actorOf(Props(new IntegratorActor(numChannels, combine)))
      val branches = system.actorOf(Props(new BranchesActor(numChannels, numColumns, H, F, integrator)))
      val source = system.actorOf(Props(new SourceActor(producer, branches)))

      // start the actors
      AkkaActorState.startActor(producer)
      AkkaActorState.startActor(source)

      // start the pipeline
      producer ! new NextMessage(source)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      AkkaActorState.awaitTermination(system)
    }
  }


  private trait Msg extends Message
  private case class NextMessage(source: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(source)
  }
  private case object BootMessage extends Msg with NoRefs
  private case object ExitMessage extends Msg with NoRefs
  private case class ValueMessage(value: Double) extends Msg with NoRefs
  private case class SourcedValueMessage(sourceId: Int, value: Double) extends Msg with NoRefs
  private case class CollectionMessage[T](values: util.Collection[T]) extends Msg with NoRefs

  private class ProducerActor(numSimulations: Int) extends AkkaActor[Msg] {

    private var numMessagesSent: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case message: FilterBankConfig.NextMessage =>
          val source = message.source.asInstanceOf[ActorRef[Msg]]
          if (numMessagesSent == numSimulations) {
            source ! ExitMessage
            exit()
          }
          else {
            source ! BootMessage
            numMessagesSent += 1
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private abstract class FilterBankActor(nextActor: ActorRef[Msg]) extends AkkaActor[Msg] {

    protected override def onPostStart() {
      AkkaActorState.startActor(nextActor)
    }

    protected override def onPostExit() {
      nextActor ! ExitMessage
    }
  }

  private class SourceActor(producer: ActorRef[Msg], nextActor: ActorRef[Msg]) extends FilterBankActor(nextActor) {

    private final val maxValue: Int = 1000
    private var current: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case _: FilterBankConfig.BootMessage =>
          nextActor ! new FilterBankConfig.ValueMessage(current)
          current = (current + 1) % maxValue
          producer ! new FilterBankConfig.NextMessage(self)
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SinkActor(printRate: Int) extends AkkaActor[Msg] {

    private var count: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          if (FilterBankConfig.debug && (count == 0)) {
            System.out.println("SinkActor: result = " + result)
          }
          count = (count + 1) % printRate
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class BranchesActor(numChannels: Int, numColumns: Int, H: Array[Array[Double]], F: Array[Array[Double]], nextActor: ActorRef[Msg]) extends AkkaActor[Msg] {

    private final val banks = Array.tabulate[ActorRef[Msg]](numChannels)(i => {
      ctx.spawnAnonymous(Behaviors.setup { ctx => new BankActor(i, numColumns, H(i), F(i), nextActor, ctx)})
    })

    protected override def onPostStart() {
      for (loopBank <- banks) {
        AkkaActorState.startActor(loopBank)
      }
    }

    protected override def onPostExit() {
      for (loopBank <- banks) {
        loopBank ! ExitMessage
      }
    }

    override def process(theMsg: Msg) {
      theMsg match {
        case _: FilterBankConfig.ValueMessage =>
          for (loopBank <- banks) {
            loopBank ! theMsg
          }
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class BankActor(sourceId: Int, numColumns: Int, H: Array[Double], F: Array[Double], integrator: ActorRef[Msg]) extends AkkaActor[Msg] {

    private final val firstActor = ctx.spawnAnonymous(Behaviors.setup { ctx => new DelayActor(sourceId + ".1", numColumns - 1,
      ctx.spawnAnonymous(Behaviors.setup { ctx => new FirFilterActor(sourceId + ".1", numColumns, H,
        ctx.spawnAnonymous(Behaviors.setup { ctx => new SampleFilterActor(numColumns,
          ctx.spawnAnonymous(Behaviors.setup { ctx => new DelayActor(sourceId + ".2", numColumns - 1,
            ctx.spawnAnonymous(Behaviors.setup { ctx => new FirFilterActor(sourceId + ".2", numColumns, F,
              ctx.spawnAnonymous(Behaviors.setup { ctx => new TaggedForwardActor(sourceId, integrator))))))))))))))))))

    protected override def onPostStart() {
      AkkaActorState.startActor(firstActor)
    }

    protected override def onPostExit() {
      firstActor ! ExitMessage
    }

    override def process(theMsg: Msg) {
      theMsg match {
        case _: FilterBankConfig.ValueMessage =>
          firstActor ! theMsg
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class DelayActor(sourceId: String, delayLength: Int, nextActor: ActorRef[Msg]) extends FilterBankActor(nextActor) {

    private final val state = Array.tabulate[Double](delayLength)(i => 0)
    private var placeHolder: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          nextActor ! new FilterBankConfig.ValueMessage(state(placeHolder))
          state(placeHolder) = result
          placeHolder = (placeHolder + 1) % delayLength
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class FirFilterActor(sourceId: String, peekLength: Int, coefficients: Array[Double], nextActor: ActorRef[Msg]) extends FilterBankActor(nextActor) {

    private var data = Array.tabulate[Double](peekLength)(i => 0)
    private var dataIndex: Int = 0
    private var dataFull: Boolean = false

    override def process(theMsg: Msg) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          data(dataIndex) = result
          dataIndex += 1
          if (dataIndex == peekLength) {
            dataFull = true
            dataIndex = 0
          }
          if (dataFull) {
            var sum: Double = 0.0
            var i: Int = 0
            while (i < peekLength) {
              sum += (data(i) * coefficients(peekLength - i - 1))
              i += 1
            }
            nextActor ! new FilterBankConfig.ValueMessage(sum)
          }
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case _ =>
      }
    }
  }

  private object SampleFilterActor {
    private final val ZERO_RESULT: FilterBankConfig.ValueMessage = new FilterBankConfig.ValueMessage(0)
  }

  private class SampleFilterActor(sampleRate: Int, nextActor: ActorRef[Msg]) extends FilterBankActor(nextActor) {

    private var samplesReceived: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case _: FilterBankConfig.ValueMessage =>
          if (samplesReceived == 0) {
            nextActor ! theMsg
          } else {
            nextActor ! SampleFilterActor.ZERO_RESULT
          }
          samplesReceived = (samplesReceived + 1) % sampleRate
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class TaggedForwardActor(sourceId: Int, nextActor: ActorRef[Msg]) extends FilterBankActor(nextActor) {

    override def process(theMsg: Msg) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          nextActor ! new FilterBankConfig.SourcedValueMessage(sourceId, result)
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class IntegratorActor(numChannels: Int, nextActor: ActorRef[Msg]) extends FilterBankActor(nextActor) {

    private final val data = new java.util.ArrayList[util.Map[Integer, Double]]
    private var exitsReceived: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case message: FilterBankConfig.SourcedValueMessage =>
          val sourceId: Int = message.sourceId
          val result: Double = message.value
          val dataSize: Int = data.size
          var processed: Boolean = false
          var i: Int = 0
          while (i < dataSize) {
            val loopMap: java.util.Map[Integer, Double] = data.get(i)
            if (!loopMap.containsKey(sourceId)) {
              loopMap.put(sourceId, result)
              processed = true
              i = dataSize
            }
            i += 1
          }
          if (!processed) {
            val newMap = new java.util.HashMap[Integer, Double]
            newMap.put(sourceId, result)
            data.add(newMap)
          }
          val firstMap: java.util.Map[Integer, Double] = data.get(0)
          if (firstMap.size == numChannels) {
            nextActor ! new FilterBankConfig.CollectionMessage[Double](firstMap.values)
            data.remove(0)
          }
        case _: FilterBankConfig.ExitMessage =>
          exitsReceived += 1
          if (exitsReceived == numChannels) {
            exit()
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class CombineActor(nextActor: ActorRef[Msg]) extends FilterBankActor(nextActor) {

    override def process(theMsg: Msg) {
      theMsg match {
        case message: FilterBankConfig.CollectionMessage[_] =>
          val result = message.values.asInstanceOf[util.Collection[Double]]
          var sum: Double = 0
          for (loopValue <- result) {
            sum += loopValue
          }
          nextActor ! new FilterBankConfig.ValueMessage(sum)
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
