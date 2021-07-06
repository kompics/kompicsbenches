package se.kth.benchmarks.akka.typed_bench

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import se.kth.benchmarks.akka.ActorSystemProvider
import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.helpers.ChameneosColour
import kompics.benchmarks.benchmarks.ChameneosRequest

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.util.Try
import java.util.concurrent.CountDownLatch
import com.typesafe.scalalogging.StrictLogging

import scala.language.postfixOps

object Chameneos extends Benchmark {

  override type Conf = ChameneosRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[ChameneosRequest])
  };
  override def newInstance(): Instance = new ChameneosI;

  class ChameneosI extends Instance with StrictLogging {
    import SystemSupervisor._;

    private var numChameneos = -1;
    private var numMeetings = -1L;
    private var system: ActorSystem[SystemMessage] = null;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      logger.info(s"Setting up Instance with config: $c");
      this.numChameneos = c.numberOfChameneos;
      this.numMeetings = c.numberOfMeetings;
      this.system = ActorSystemProvider.newTypedActorSystem[SystemMessage](SystemSupervisor(), "typed_chameneos");
    }
    override def prepareIteration(): Unit = {
      assert(this.system != null);
      logger.debug("Preparing iteration");
      this.latch = new CountDownLatch(1);
      implicit val timeout: Timeout = 3.seconds;
      implicit val scheduler = system.scheduler;
      val f: Future[OperationSucceeded.type] =
        system.ask(ref => StartActors(ref, this.numMeetings, this.numChameneos, latch));
      implicit val ec = system.executionContext;
      Await.result(f, 3 seconds);
    }
    override def runIteration(): Unit = {
      assert(this.latch != null);
      this.system ! RunIteration;
      latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up iteration");
      if (this.latch != null) {
        this.latch = null;
      }
      implicit val timeout: Timeout = 3.seconds;
      implicit val scheduler = system.scheduler;
      val f: Future[OperationSucceeded.type] = system.ask(ref => StopActors(ref));
      implicit val ec = system.executionContext;
      Await.result(f, 3 seconds);
      if (lastIteration) {
        system ! GracefulShutdown
        Await.ready(system.whenTerminated, 5.second);
        system = null;
        logger.info("Cleaned up Instance");
      }
    }
  }

  object SystemSupervisor {
    sealed trait SystemMessage;
    case class StartActors(replyTo: ActorRef[OperationSucceeded.type],
                           numMeetings: Long,
                           numChameneos: Int,
                           latch: CountDownLatch)
        extends SystemMessage;
    case class StopActors(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMessage;
    case object RunIteration extends SystemMessage;
    case object GracefulShutdown extends SystemMessage;
    case object OperationSucceeded;

    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new SystemSupervisor(context));
  }

  class SystemSupervisor(context: ActorContext[SystemSupervisor.SystemMessage])
      extends AbstractBehavior[SystemSupervisor.SystemMessage](context) {
    import SystemSupervisor._;

    private var mall: ActorRef[MallMsg] = null;
    private var chameneos: List[ActorRef[ChameneoMsg]] = Nil;
    var run_id: Int = -1

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case StartActors(replyTo, numMeetings, numChameneos, latch) => {
          run_id += 1;
          this.mall = context.spawn(ChameneosMallActor(numMeetings, numChameneos, latch), s"mall-$run_id");
          this.chameneos = (for (i <- 0 until numChameneos) yield {
            val initialColour = ChameneosColour.forId(i);
            context.spawn(ChameneoActor(mall, initialColour), s"chameneo-$i-$run_id")
          }).toList;
          replyTo ! OperationSucceeded;
          this
        }
        case RunIteration => {
          assert(this.chameneos != Nil);
          this.chameneos.foreach { chameneo =>
            chameneo ! Messages.Run
          }
          this
        }
        case StopActors(replyTo) => {
          this.chameneos = Nil; // they stop themselves
          if (this.mall != null) {
            context.stop(this.mall);
            this.mall = null;
          }
          replyTo ! OperationSucceeded;
          this
        }
        case GracefulShutdown => {
          Behaviors.stopped
        }
      }
    }
  }

  sealed trait MallMsg;
  sealed trait ChameneoMsg;
  object Messages {
    case class MeetingCount(count: Long) extends MallMsg;
    case class Meet(colour: ChameneosColour, chameneo: ActorRef[ChameneoMsg]) extends MallMsg with ChameneoMsg;
    case class Change(colour: ChameneosColour) extends ChameneoMsg;
    case object Exit extends ChameneoMsg;
    case object Run extends ChameneoMsg;
  }

  object ChameneosMallActor {
    def apply(numMeetings: Long, numChameneos: Int, latch: CountDownLatch): Behavior[MallMsg] =
      Behaviors.setup(context => new ChameneosMallActor(context, numMeetings, numChameneos, latch));
  }
  class ChameneosMallActor(context: ActorContext[MallMsg],
                           val numMeetings: Long,
                           val numChameneos: Int,
                           val latch: CountDownLatch)
      extends AbstractBehavior[MallMsg](context) {
    import Messages._;

    context.setLoggerName(this.getClass);
    val log = context.log;
    val selfRef = context.self

    private var waitingChameneo: Option[ActorRef[ChameneoMsg]] = None;
    private var sumMeetings: Long = 0L;
    private var meetingsCount: Long = 0L;
    private var numFaded: Int = 0;

    override def onMessage(msg: MallMsg): Behavior[MallMsg] = {
      msg match {
        case MeetingCount(count) => {
          this.numFaded += 1;
          this.sumMeetings += count;
          if (this.numFaded == this.numChameneos) {
            latch.countDown();
            log.info("Done!");
          }
          this
        }
        case msg @ Meet(_, chameneo) => {
          if (meetingsCount < numMeetings) {
            waitingChameneo match {
              case Some(other) => {
                this.meetingsCount += 1;
                other ! msg;
                waitingChameneo = None;
              }
              case None => {
                waitingChameneo = Some(chameneo);
              }
            }
          } else {
            chameneo ! Exit;
          }
          this
        }
        case _ => ???
      }
    }
  }
  object ChameneoActor {
    def apply(mall: ActorRef[MallMsg], initialColour: ChameneosColour): Behavior[ChameneoMsg] =
      Behaviors.setup(context => new ChameneoActor(context, mall, initialColour));
  }

  class ChameneoActor(context: ActorContext[ChameneoMsg],
                      val mall: ActorRef[MallMsg],
                      initialColour: ChameneosColour)
      extends AbstractBehavior[ChameneoMsg](context) {
    import Messages._;

    context.setLoggerName(this.getClass);
    val log = context.log;
    val selfRef = context.self

    private var colour = initialColour;
    private var meetings: Long = 0L;

    override def onMessage(msg: ChameneoMsg): Behavior[ChameneoMsg] = {
      msg match {
        case Run => {
          mall ! Meet(colour, selfRef);
          this
        }
        case Meet(otherColour, chameneo) => {
          colour = colour.complement(otherColour);
          meetings += 1L;
          chameneo ! Change(colour);
          mall ! Meet(colour, selfRef);
          this
        }
        case Change(newColour) => {
          colour = newColour;
          meetings += 1L;
          mall ! Meet(colour, selfRef);
          this
        }
        case Exit => {
          colour = ChameneosColour.Faded;
          mall ! MeetingCount(meetings);
          Behaviors.stopped
        }
      }
    }
  }
}
