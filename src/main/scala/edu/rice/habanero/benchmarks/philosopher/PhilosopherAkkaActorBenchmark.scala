package edu.rice.habanero.benchmarks.philosopher

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PhilosopherAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PhilosopherAkkaActorBenchmark)
  }

  private final class PhilosopherAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PhilosopherConfig.parseArgs(args)
    }

    def printArgInfo() {
      PhilosopherConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("Philosopher")

      val counter = new AtomicLong(0)


      val arbitrator = system.actorOf(Props(new ArbitratorActor(PhilosopherConfig.N)))

      val philosophers = Array.tabulate[ActorRef[Msg]](PhilosopherConfig.N)(i => {
        val loopActor = system.actorOf(Props(new PhilosopherActor(i, PhilosopherConfig.M, counter, arbitrator)))
        loopActor
      })

      philosophers.foreach(loopActor => {
        loopActor ! StartMessage()
      })

      AkkaActorState.awaitTermination(system)

      println("  Num retries: " + counter.get())
      track("Avg. Retry Count", counter.get())
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  trait Msg extends Message
  private case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(actor)
  }
  case class StartMessage() extends Msg with NoRefs

  case class ExitMessage() extends Msg with NoRefs

  case class HungryMessage(philosopher: ActorRef[Msg], philosopherId: Int) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(philosopher)
  }

  case class DoneMessage(philosopherId: Int) extends Msg with NoRefs

  case class EatMessage() extends Msg with NoRefs

  case class DeniedMessage() extends Msg with NoRefs


  private class PhilosopherActor(id: Int, rounds: Int, counter: AtomicLong, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var arbitrator: ActorRef[Msg] = _
    private var localCounter = 0L
    private var roundsSoFar = 0

    private val myHungryMessage = HungryMessage(ctx.self, id)
    private val myDoneMessage = DoneMessage(id)

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) => this.arbitrator = x

        case dm: DeniedMessage =>

          localCounter += 1
          arbitrator ! myHungryMessage

        case em: EatMessage =>

          roundsSoFar += 1
          counter.addAndGet(localCounter)

          arbitrator ! myDoneMessage
          if (roundsSoFar < rounds) {
            ctx.self ! StartMessage()
          } else {
            arbitrator ! ExitMessage()
            exit()
          }

        case sm: StartMessage =>

          arbitrator ! myHungryMessage

      }
    }
  }

  private class ArbitratorActor(numForks: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val forks = Array.tabulate(numForks)(i => new AtomicBoolean(false))
    private var numExitedPhilosophers = 0

    override def process(msg: Msg) {
      msg match {
        case hm: HungryMessage =>

          val leftFork = forks(hm.philosopherId)
          val rightFork = forks((hm.philosopherId + 1) % numForks)

          if (leftFork.get() || rightFork.get()) {
            // someone else has access to the fork
            hm.philosopher ! DeniedMessage()
          } else {
            leftFork.set(true)
            rightFork.set(true)
            hm.philosopher ! EatMessage()
          }

        case dm: DoneMessage =>

          val leftFork = forks(dm.philosopherId)
          val rightFork = forks((dm.philosopherId + 1) % numForks)
          leftFork.set(false)
          rightFork.set(false)

        case em: ExitMessage =>

          numExitedPhilosophers += 1
          if (numForks == numExitedPhilosophers) {
            exit()
          }
      }
    }
  }

}
