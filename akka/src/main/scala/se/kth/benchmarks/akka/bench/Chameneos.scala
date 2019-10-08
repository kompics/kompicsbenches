package se.kth.benchmarks.akka.bench

import se.kth.benchmarks.akka.ActorSystemProvider
import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.helpers.ChameneosColour
import kompics.benchmarks.benchmarks.ChameneosRequest
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import java.util.concurrent.CountDownLatch
import com.typesafe.scalalogging.StrictLogging

object Chameneos extends Benchmark {

  override type Conf = ChameneosRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[ChameneosRequest])
  };
  override def newInstance(): Instance = new ChameneosI;

  class ChameneosI extends Instance with StrictLogging {

    private var numChameneos = -1;
    private var numMeetings = -1L;
    private var system: ActorSystem = null;
    private var mall: ActorRef = null;
    private var chameneos: List[ActorRef] = Nil;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      logger.info("Setting up Instance");
      this.numChameneos = c.numberOfChameneos;
      this.numMeetings = c.numberOfMeetings;
      this.system = ActorSystemProvider.newActorSystem(name = "chameneos");
    }
    override def prepareIteration(): Unit = {
      assert(this.system != null);
      logger.debug("Preparing iteration");
      this.latch = new CountDownLatch(1);
      this.mall = system.actorOf(Props(new ChameneosMallActor(numMeetings, numChameneos, latch)));
      this.chameneos = (for (i <- 0 until this.numChameneos) yield {
        val initialColour = ChameneosColour.forId(i);
        system.actorOf(Props(new ChameneoActor(mall, initialColour)))
      }).toList;
    }
    override def runIteration(): Unit = {
      assert(this.latch != null);
      assert(this.chameneos != Nil);
      this.chameneos.foreach { chameneo =>
        chameneo ! Messages.Run
      }
      latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up iteration");
      if (this.latch != null) {
        this.latch = null;
      }
      this.chameneos = Nil; // they stop themselves
      if (this.mall != null) {
        assert(this.system != null);
        system.stop(this.mall);
        this.mall = null;
      }
      if (lastIteration) {
        val f = system.terminate();
        Await.ready(f, 5.second);
        system = null;
        logger.info("Cleaned up Instance");
      }
    }
  }

  sealed trait MallMsg;
  sealed trait ChameneoMsg;
  object Messages {
    case class MeetingCount(count: Long) extends MallMsg;
    case class Meet(colour: ChameneosColour, chameneo: ActorRef) extends MallMsg with ChameneoMsg;
    case class Change(colour: ChameneosColour) extends ChameneoMsg;
    case object Exit extends ChameneoMsg;
    case object Run extends ChameneoMsg;
  }

  class ChameneosMallActor(val numMeetings: Long, val numChameneos: Int, val latch: CountDownLatch)
      extends Actor
      with ActorLogging {
    import Messages._;

    private var waitingChameneo: Option[ActorRef] = None;
    private var sumMeetings: Long = 0L;
    private var meetingsCount: Long = 0L;
    private var numFaded: Int = 0;

    override def receive = {
      case MeetingCount(count) => {
        this.numFaded += 1;
        this.sumMeetings += count;
        if (this.numFaded == this.numChameneos) {
          latch.countDown();
          log.info("Done!");
        }
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
          sender() ! Exit;
        }
      }
      case _ => ???
    }
  }

  class ChameneoActor(val mall: ActorRef, initialColour: ChameneosColour) extends Actor with ActorLogging {
    import Messages._;

    private var colour = initialColour;
    private var meetings: Long = 0L;

    override def receive = {
      case Run => {
        mall ! Meet(colour, self);
      }
      case Meet(otherColour, chameneo) => {
        colour = colour.complement(otherColour);
        meetings += 1L;
        chameneo ! Change(colour);
        mall ! Meet(colour, self);
      }
      case Change(newColour) => {
        colour = newColour;
        meetings += 1L;
        mall ! Meet(colour, self);
      }
      case Exit => {
        colour = ChameneosColour.Faded;
        mall ! MeetingCount(meetings);
        context.stop(self);
      }
    }
  }

}
