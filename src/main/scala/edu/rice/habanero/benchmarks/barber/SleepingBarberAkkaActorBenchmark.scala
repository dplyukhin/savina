package edu.rice.habanero.benchmarks.barber

import java.util.concurrent.atomic.AtomicLong

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import scala.collection.mutable.ListBuffer

/**
 * source: https://code.google.com/p/gparallelizer/wiki/ActorsExamples
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SleepingBarberAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SleepingBarberAkkaActorBenchmark)
  }

  private final class SleepingBarberAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SleepingBarberConfig.parseArgs(args)
    }

    def printArgInfo() {
      SleepingBarberConfig.printArgs()
    }

    def runIteration() {

      val idGenerator = new AtomicLong(0)

      val system = AkkaActorState.newActorSystem("SleepingBarber")

      val barber = system.actorOf(Props(new BarberActor()))
      val room = system.actorOf(Props(new WaitingRoomActor(SleepingBarberConfig.W, barber)))
      val factoryActor = system.actorOf(Props(new CustomerFactoryActor(idGenerator, SleepingBarberConfig.N, room)))

      AkkaActorState.startActor(barber)
      AkkaActorState.startActor(room)
      AkkaActorState.startActor(factoryActor)

      factoryActor ! Start

      AkkaActorState.awaitTermination(system)

      track("CustomerAttempts", idGenerator.get())
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }


  trait Msg extends Message
  case object Full extends Msg with NoRefs
  case object Wait extends Msg with NoRefs
  case object Next extends Msg with NoRefs
  case object Start extends Msg with NoRefs
  case object Done extends Msg with NoRefs
  case object Exit extends Msg with NoRefs
  case class Enter(customer: ActorRef[Msg], room: ActorRef[Msg]) extends Msg {
    def refs: Iterable[ActorRef[_]] = List(customer, room)
  }
  case class Returned(customer: ActorRef[Msg]) extends Msg {
    def refs: Iterable[ActorRef[_]] = List(customer)
  }


  private class WaitingRoomActor(capacity: Int, barber: ActorRef, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val waitingCustomers = new ListBuffer[ActorRef]()
    private var barberAsleep = true

    override def process(msg: Msg) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          if (waitingCustomers.size == capacity) {

            customer ! Full

          } else {

            waitingCustomers.append(customer)
            if (barberAsleep) {

              barberAsleep = false
              self ! Next

            } else {

              customer ! Wait
            }
          }

        case Next =>

          if (waitingCustomers.size > 0) {

            val customer = waitingCustomers.remove(0)
            barber ! new Enter(customer, self)

          } else {

            barber ! Wait
            barberAsleep = true

          }

        case message: Exit =>

          barber ! Exit
          exit()

      }
    }
  }

  private class BarberActor(ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val random = new PseudoRandom()

    override def process(msg: Msg) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          val room = message.room

          customer ! Start
          // println("Barber: Processing customer " + customer)
          SleepingBarberConfig.busyWait(random.nextInt(SleepingBarberConfig.AHR) + 10)
          customer ! Done
          room ! Next

        case Wait =>

        // println("Barber: No customers. Going to have a sleep")

        case Exit =>

          exit()

      }
    }
  }

  private class CustomerFactoryActor(idGenerator: AtomicLong, haircuts: Int, room: ActorRef[Msg],
                                     ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val random = new PseudoRandom()
    private var numHairCutsSoFar = 0

    override def process(msg: Msg) {
      msg match {
        case Start =>

          var i = 0
          while (i < haircuts) {
            sendCustomerToRoom()
            SleepingBarberConfig.busyWait(random.nextInt(SleepingBarberConfig.APR) + 10)
            i += 1
          }

        case message: Returned =>

          idGenerator.incrementAndGet()
          sendCustomerToRoom(message.customer)

        case Done =>

          numHairCutsSoFar += 1
          if (numHairCutsSoFar == haircuts) {
            println("Total attempts: " + idGenerator.get())
            room ! Exit
            exit()
          }
      }
    }

    private def sendCustomerToRoom() {
      val customer = context.system.actorOf(Props(new CustomerActor(idGenerator.incrementAndGet(), self)))
      AkkaActorState.startActor(customer)

      sendCustomerToRoom(customer)
    }

    private def sendCustomerToRoom(customer: ActorRef) {
      val enterMessage = new Enter(customer, room)
      room ! enterMessage
    }
  }

  private class CustomerActor(val id: Long, factoryActor: ActorRef[Msg], ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    override def process(msg: Msg) {
      msg match {
        case Full =>

          // println("Customer-" + id + " The waiting room is full. I am leaving.")
          factoryActor ! new Returned(self)

        case Wait =>

        // println("Customer-" + id + " I will wait.")

        case Start =>

        // println("Customer-" + id + " I am now being served.")

        case Done =>

          //  println("Customer-" + id + " I have been served.")
          factoryActor ! Done
          exit()
      }
    }
  }

}
