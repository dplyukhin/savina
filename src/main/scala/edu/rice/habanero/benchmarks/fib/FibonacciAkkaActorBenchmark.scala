package edu.rice.habanero.benchmarks.fib



import gc.{ActorContext, ActorFactory, ActorRef, Behavior, Behaviors, Message}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, AkkaMsg, BenchmarkMessage}
import edu.rice.habanero.actors.AkkaImplicits._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import akka.actor.typed.{Behavior => AkkaBehavior}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FibonacciAkkaActorBenchmark {

  trait NoRefsMessage extends Message {
    override def refs: Iterable[ActorRef[Nothing]] = Seq()
  }
  sealed trait FibMessage extends Message
  final case class Request(parent: Option[ActorRef[AkkaMsg[Response]]], n: Int) extends FibMessage {
    def refs: Iterable[ActorRef[AkkaMsg[Response]]] = parent.toList
  }
  final case class Response(value: Int) extends FibMessage with NoRefsMessage

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FibonacciAkkaActorBenchmark)
  }

  private final class FibonacciAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FibonacciConfig.parseArgs(args)
    }

    def printArgInfo() {
      FibonacciConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("Fibonacci", FibonacciActor.createRoot())

      AkkaActorState.startActor(system)
      system ! Request(None, FibonacciConfig.N)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }
  object FibonacciActor {
    def createRoot(): AkkaBehavior[AkkaMsg[FibMessage]] = {
      Behaviors.setupReceptionist(context => new FibonacciActor(context))
    }
    def apply(): ActorFactory[AkkaMsg[FibMessage]] = {
      Behaviors.setup(context => new FibonacciActor(context))
    }

  }

  private val RESPONSE_ONE = Response(1)


  private class FibonacciActor(context: ActorContext[AkkaMsg[FibMessage]])
    extends AkkaActor[FibMessage](context) {

    private var result = 0
    private var respReceived = 0
    private var parent: Option[ActorRef[AkkaMsg[Response]]] = None

    override def process(msg: FibMessage): Behavior[AkkaMsg[FibMessage]] = {

      msg match {
        case Request(parent, n) =>
          this.parent = parent

          if (n <= 2) {

            result = 1
            processResult(RESPONSE_ONE)

          } else {

            val f1 = context.spawn(FibonacciActor(), "Actor_f1")
            val self1 = context.createRef(context.self, f1)
            f1 ! Request(Some(self1), n - 1)

            val f2 = context.spawn(FibonacciActor(), "Actor_f2")
            val self2 = context.createRef(context.self, f2)
            f2 ! Request(Some(self2), n - 2)

            context.release(Seq(f1, f2))

          }

          this

        case Response(value) =>

          respReceived += 1
          result += value

          if (respReceived == 2) {
            processResult(Response(result))
          }
          else {
            this
          }
      }
    }

    private def processResult(response: Response): Behavior[AkkaMsg[FibMessage]] = {
      parent match {
        case None =>
          println(" Result = " + result)
        case Some(actor) =>
          actor ! response
          context.release(actor)
      }
      exit()
    }
  }

}
