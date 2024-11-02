package edu.rice.habanero.benchmarks.logmap

import java.util

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.logmap.LogisticMapConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object LogisticMapAkkaManualStashActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new LogisticMapAkkaManualStashActorBenchmark)
  }

  private final class LogisticMapAkkaManualStashActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      LogisticMapConfig.parseArgs(args)
    }

    def printArgInfo() {
      LogisticMapConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("LogisticMap")

      val master = system.actorOf(Props(new Master()))
      AkkaActorState.startActor(master)
      master ! StartMessage

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message
  private case object StartMsg extends Msg with NoRefs
  private case object StopMsg extends Msg with NoRefs
  private case object NextTermMsg extends Msg with NoRefs
  private case object GetTermMsg extends Msg with NoRefs
  private case class ComputeMsg(sender: ActorRef[Msg], term: Double) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(sender)
  }
  private case class ResultMsg(term: Double) extends Msg with NoRefs

  private class Master extends GCActor[Msg](ctx) {

    private final val numComputers: Int = LogisticMapConfig.numSeries
    private final val computers = Array.tabulate[ActorRef[Msg]](numComputers)(i => {
      val rate = LogisticMapConfig.startRate + (i * LogisticMapConfig.increment)
      ctx.spawnAnonymous(Behaviors.setup { ctx => new RateComputer(rate, ctx)})
    })

    private final val numWorkers: Int = LogisticMapConfig.numSeries
    private final val workers = Array.tabulate[ActorRef[Msg]](numWorkers)(i => {
      val rateComputer = computers(i % numComputers)
      val startTerm = i * LogisticMapConfig.increment
      ctx.spawnAnonymous(Behaviors.setup { ctx => new SeriesWorker(i, self, rateComputer, startTerm, ctx)})
    })

    private var numWorkRequested: Int = 0
    private var numWorkReceived: Int = 0
    private var termsSum: Double = 0

    protected override def onPostStart() {
      computers.foreach(loopComputer => {
        AkkaActorState.startActor(loopComputer)
      })
      workers.foreach(loopWorker => {
        AkkaActorState.startActor(loopWorker)
      })
    }

    override def process(theMsg: Msg) {
      theMsg match {
        case _: StartMessage =>

          var i: Int = 0
          while (i < LogisticMapConfig.numTerms) {
            // request each worker to compute the next term
            workers.foreach(loopWorker => {
              loopWorker ! NextTermMessage
            })
            i += 1
          }

          // workers should stop after all items have been computed
          workers.foreach(loopWorker => {
            loopWorker ! GetTermMessage
            numWorkRequested += 1
          })

        case rm: ResultMessage =>

          termsSum += rm.term
          numWorkReceived += 1

          if (numWorkRequested == numWorkReceived) {

            println("Terms sum: " + termsSum)

            computers.foreach(loopComputer => {
              loopComputer ! StopMessage
            })
            workers.foreach(loopWorker => {
              loopWorker ! StopMessage
            })
            exit()
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SeriesWorker(id: Int, master: ActorRef[Msg], computer: ActorRef[Msg], startTerm: Double, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val curTerm = Array.tabulate[Double](1)(i => startTerm)

    private var inReplyMode = false
    private val stashedMessages = new util.LinkedList[Msg]()

    override def process(theMsg: Msg) {

      if (inReplyMode) {

        theMsg match {

          case resultMessage: ResultMessage =>

            inReplyMode = false
            curTerm(0) = resultMessage.term

          case message =>

            stashedMessages.add(message)
        }

      } else {

        // process the message
        theMsg match {

          case computeMessage: NextTermMessage =>

            val sender = self
            val newMessage = new ComputeMessage(sender, curTerm(0))
            computer ! newMessage
            inReplyMode = true

          case message: GetTermMessage =>

            // do not reply to master if stash is not empty
            if (stashedMessages.isEmpty) {
              master ! new ResultMessage(curTerm(0))
            } else {
              stashedMessages.add(message)
            }

          case _: StopMessage =>

            exit()

          case message =>
            val ex = new IllegalArgumentException("Unsupported message: " + message)
            ex.printStackTrace(System.err)
        }
      }

      // recycle stashed messages
      if (!inReplyMode && !stashedMessages.isEmpty) {
        val newMsg = stashedMessages.remove(0)
        self ! newMsg
      }
    }
  }

  private class RateComputer(rate: Double, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    override def process(theMsg: Msg) {
      theMsg match {
        case computeMessage: ComputeMessage =>

          val result = computeNextTerm(computeMessage.term, rate)
          val resultMessage = new ResultMessage(result)
          sender() ! resultMessage

        case _: StopMessage =>

          exit()

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
