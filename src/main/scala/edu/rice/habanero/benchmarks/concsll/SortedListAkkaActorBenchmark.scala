package edu.rice.habanero.benchmarks.concsll

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SortedListAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SortedListAkkaActorBenchmark)
  }

  private final class SortedListAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SortedListConfig.parseArgs(args)
    }

    def printArgInfo() {
      SortedListConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = SortedListConfig.NUM_ENTITIES
      val numMessagesPerWorker: Int = SortedListConfig.NUM_MSGS_PER_WORKER
      val system = AkkaActorState.newActorSystem("SortedList")

      val master = system.actorOf(Props(new Master(numWorkers, numMessagesPerWorker)))
      AkkaActorState.startActor(master)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message
  private case class WriteMessage(sender: ActorRef[Msg], value: Int) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender)
  }
  private case class ContainsMessage(sender: ActorRef[Msg], value: Int) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender)
  }
  private case class SizeMessage(sender: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender)
  }
  private case class ResultMessage(sender: ActorRef[Msg], value: Int) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender)
  }
  private case object DoWorkMessage extends Msg with NoRefs
  private case object EndWorkMessage extends Msg with NoRefs

  private class Master(numWorkers: Int, numMessagesPerWorker: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val workers = new Array[ActorRef[Msg]](numWorkers)
    private final val sortedList = ctx.spawnAnonymous(Behaviors.setup { ctx => new SortedList(ctx)})
    ctx.sp
    private var numWorkersTerminated: Int = 0

    override def onPostStart() {
      AkkaActorState.startActor(sortedList)

      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = ctx.spawnAnonymous(Behaviors.setup { ctx => new Worker(self, sortedList, i, numMessagesPerWorker, ctx)})
        AkkaActorState.startActor(workers(i))
        workers(i) ! DoWorkMessage
        i += 1
      }
    }

    override def process(msg: Msg) {
      if (msg.isInstanceOf[SortedListConfig.EndWorkMessage]) {
        numWorkersTerminated += 1
        if (numWorkersTerminated == numWorkers) {
          sortedList ! EndWorkMessage
          exit()
        }
      }
    }
  }

  private class Worker(master: ActorRef[Msg], sortedList: ActorRef[Msg], id: Int, numMessagesPerWorker: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val writePercent = SortedListConfig.WRITE_PERCENTAGE
    private final val sizePercent = SortedListConfig.SIZE_PERCENTAGE
    private var messageCount: Int = 0
    private final val random = new PseudoRandom(id + numMessagesPerWorker + writePercent + sizePercent)

    override def process(msg: Msg) {
      messageCount += 1
      if (messageCount <= numMessagesPerWorker) {
        val anInt: Int = random.nextInt(100)
        if (anInt < sizePercent) {
          sortedList ! new SortedListConfig.SizeMessage(self)
        } else if (anInt < (sizePercent + writePercent)) {
          sortedList ! new SortedListConfig.WriteMessage(self, random.nextInt)
        } else {
          sortedList ! new SortedListConfig.ContainsMessage(self, random.nextInt)
        }
      } else {
        master ! EndWorkMessage
        exit()
      }
    }
  }

  private class SortedList extends GCActor[Msg](ctx) {

    private[concsll] final val dataList = new SortedLinkedList[Integer]

    override def process(msg: Msg) {
      msg match {
        case writeMessage: SortedListConfig.WriteMessage =>
          val value: Int = writeMessage.value
          dataList.add(value)
          val sender = writeMessage.sender.asInstanceOf[ActorRef[Msg]]
          sender ! new SortedListConfig.ResultMessage(self, value)
        case containsMessage: SortedListConfig.ContainsMessage =>
          val value: Int = containsMessage.value
          val result: Int = if (dataList.contains(value)) 1 else 0
          val sender = containsMessage.sender.asInstanceOf[ActorRef[Msg]]
          sender ! new SortedListConfig.ResultMessage(self, result)
        case readMessage: SortedListConfig.SizeMessage =>
          val value: Int = dataList.size
          val sender = readMessage.sender.asInstanceOf[ActorRef[Msg]]
          sender ! new SortedListConfig.ResultMessage(self, value)
        case _: SortedListConfig.EndWorkMessage =>
          printf(BenchmarkRunner.argOutputFormat, "List Size", dataList.size)
          exit()
        case _ =>
          System.err.println("Unsupported message: " + msg)
      }
    }
  }

}
