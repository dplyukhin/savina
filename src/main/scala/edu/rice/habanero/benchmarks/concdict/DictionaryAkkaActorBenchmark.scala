package edu.rice.habanero.benchmarks.concdict

import java.util

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.concdict.DictionaryConfig.DATA_LIMIT
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object DictionaryAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new DictionaryAkkaActorBenchmark)
  }

  private final class DictionaryAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      DictionaryConfig.parseArgs(args)
    }

    def printArgInfo() {
      DictionaryConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = DictionaryConfig.NUM_ENTITIES
      val numMessagesPerWorker: Int = DictionaryConfig.NUM_MSGS_PER_WORKER
      val system = AkkaActorState.newActorSystem("Dictionary")

      val master = system.actorOf(Props(new Master(numWorkers, numMessagesPerWorker)))
      AkkaActorState.startActor(master)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message
  private case class WriteMessage(sender: ActorRef[Msg], _key: Int, value: Int) extends Msg {
    val key = Math.abs(_key) % DATA_LIMIT
    override def refs: Iterable[ActorRef[_]] = Some(sender)
  }
  private case class ReadMessage(sender: ActorRef[Msg], _key: Int) extends Msg {
    val key = Math.abs(_key) % DATA_LIMIT
    override def refs: Iterable[ActorRef[_]] = Some(sender)
  }
  private case class ResultMessage(sender: ActorRef[Msg], key: Int) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(sender)
  }
  private case object DoWorkMessage extends Msg with NoRefs
  private case object EndWorkMessage extends Msg with NoRefs


  private class Master(numWorkers: Int, numMessagesPerWorker: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val workers = new Array[ActorRef[Msg]](numWorkers)
    private final val dictionary = ctx.spawnAnonymous(Behaviors.setup { ctx => new Dictionary(DictionaryConfig.DATA_MAP, ctx)})
    private var numWorkersTerminated: Int = 0

    override def onPostStart() {
      AkkaActorState.startActor(dictionary)

      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = ctx.spawnAnonymous(Behaviors.setup { ctx => new Worker(self, dictionary, i, numMessagesPerWorker, ctx)})
        AkkaActorState.startActor(workers(i))
        workers(i) ! DoWorkMessage
        i += 1
      }
    }

    override def process(msg: Msg) {
      if (msg.isInstanceOf[DictionaryConfig.EndWorkMessage]) {
        numWorkersTerminated += 1
        if (numWorkersTerminated == numWorkers) {
          dictionary ! EndWorkMessage
          exit()
        }
      }
    }
  }

  private class Worker(master: ActorRef[Msg], dictionary: ActorRef[Msg], id: Int, numMessagesPerWorker: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val writePercent = DictionaryConfig.WRITE_PERCENTAGE
    private var messageCount: Int = 0
    private final val random = new util.Random(id + numMessagesPerWorker + writePercent)

    override def process(msg: Msg) {
      messageCount += 1
      if (messageCount <= numMessagesPerWorker) {
        val anInt: Int = random.nextInt(100)
        if (anInt < writePercent) {
          dictionary ! new WriteMessage(self, random.nextInt, random.nextInt)
        } else {
          dictionary ! new ReadMessage(self, random.nextInt)
        }
      } else {
        master ! EndWorkMessage
        exit()
      }
    }
  }

  private class Dictionary(initialState: util.Map[Integer, Integer], ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private[concdict] final val dataMap = new util.HashMap[Integer, Integer](initialState)

    override def process(msg: Msg) {
      msg match {
        case writeMessage: DictionaryConfig.WriteMessage =>
          val key = writeMessage.key
          val value = writeMessage.value
          dataMap.put(key, value)
          val sender = writeMessage.sender.asInstanceOf[ActorRef[Msg]]
          sender ! new DictionaryConfig.ResultMessage(self, value)
        case readMessage: DictionaryConfig.ReadMessage =>
          val value = dataMap.get(readMessage.key)
          val sender = readMessage.sender.asInstanceOf[ActorRef[Msg]]
          sender ! new DictionaryConfig.ResultMessage(self, value)
        case _: DictionaryConfig.EndWorkMessage =>
          printf(BenchmarkRunner.argOutputFormat, "Dictionary Size", dataMap.size)
          exit()
        case _ =>
          System.err.println("Unsupported message: " + msg)
      }
    }
  }

}
