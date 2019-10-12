package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider}
import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.helpers.ChameneosColour
import kompics.benchmarks.benchmarks.ChameneosRequest
import se.sics.kompics.{Component, Fault, Kill, Killed, Kompics, KompicsEvent, Start, Started}
import se.sics.kompics.sl._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Try
import java.util.concurrent.CountDownLatch
import com.typesafe.scalalogging.StrictLogging
import java.util.UUID
import se.kth.benchmarks.kompicsscala.bench.Chameneos.Messages.MeetingCount

object Chameneos extends Benchmark {

  override type Conf = ChameneosRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[ChameneosRequest])
  };
  override def newInstance(): Instance = new ChameneosI;

  class ChameneosI extends Instance with StrictLogging {
    import scala.concurrent.ExecutionContext.Implicits.global;

    private var numChameneos = -1;
    private var numMeetings = -1L;
    private var system: KompicsSystem = null;
    private var mall: UUID = null;
    private var chameneos: List[UUID] = Nil;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      logger.info(s"Setting up Instance with config: $c");
      this.numChameneos = c.numberOfChameneos;
      this.numMeetings = c.numberOfMeetings;
      this.system = KompicsSystemProvider.newKompicsSystem();
    }
    override def prepareIteration(): Unit = {
      assert(this.system != null);
      logger.debug("Preparing iteration");
      this.latch = new CountDownLatch(1);
      val f: Future[(UUID, List[UUID])] = for {
        mallId <- system.createNotify[ChameneosMallComponent](
          Init[ChameneosMallComponent](numMeetings, numChameneos, latch)
        );
        chameneosIds <- Future.sequence((for (i <- 0 until this.numChameneos) yield {
          val initialColour = ChameneosColour.forId(i);
          system.createNotify[ChameneoComponent](Init[ChameneoComponent](initialColour))
        }).toList);
        _ <- Future.sequence(chameneosIds.flatMap(cid => {
          val mallConnF = system.connectComponents[MallPort.type](cid, mallId);
          val subConnF: List[Future[Unit]] = chameneosIds.map(cid2 => {
            if (cid != cid2) {
              system.connectComponents[ChameneoPort.type](cid2, cid);
            } else {
              Future.successful(())
            }
          });
          mallConnF :: subConnF
        }));
        _ <- system.startNotify(mallId)
      } yield (mallId, chameneosIds);
      val (mallId, chamIds) = Await.result(f, Duration.Inf);
      this.mall = mallId;
      this.chameneos = chamIds;
    }
    override def runIteration(): Unit = {
      assert(this.system != null);
      assert(this.latch != null);
      assert(this.chameneos != Nil);
      val f = Future.sequence(chameneos.map(cid => system.startNotify(cid)));
      Await.result(f, Duration.Inf);
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
        val f = system.killNotify(this.mall);
        Await.result(f, Duration.Inf);
        this.mall = null;
      }
      if (lastIteration) {
        system.terminate();
        system = null;
        logger.info("Cleaned up Instance");
      }
    }
  }

  object Messages {
    case class MeetingCount(count: Long) extends KompicsEvent;

    case class MeetMe(colour: ChameneosColour, chameneo: UUID) extends KompicsEvent;
    case class MeetUp(colour: ChameneosColour, chameneo: UUID, target: UUID) extends KompicsEvent;

    case class Change(colour: ChameneosColour, target: UUID) extends KompicsEvent;
    case object Exit extends KompicsEvent;
  }

  object MallPort extends Port {
    import Messages._;
    request[MeetMe];
    request[MeetingCount];
    indication[MeetUp];
    indication(Exit);
  }
  object ChameneoPort extends Port {
    import Messages._;
    request[Change];
  }

  class ChameneosMallComponent(init: Init[ChameneosMallComponent]) extends ComponentDefinition {
    import Messages._;

    val mallPort = provides(MallPort);

    val Init(numMeetings: Long, numChameneos: Int, latch: CountDownLatch) = init;

    private var waitingChameneo: Option[UUID] = None;
    private var sumMeetings: Long = 0L;
    private var meetingsCount: Long = 0L;
    private var numFaded: Int = 0;
    private var sentExit: Boolean = false;

    mallPort uponEvent {
      case MeetingCount(count) => {
        this.numFaded += 1;
        this.sumMeetings += count;
        if (this.numFaded == this.numChameneos) {
          latch.countDown();
          log.info("Done!");
        }
      }
      case MeetMe(colour, chameneo) => {
        if (meetingsCount < numMeetings) {
          waitingChameneo match {
            case Some(other) => {
              this.meetingsCount += 1;
              trigger(MeetUp(colour, other, chameneo) -> mallPort);
              waitingChameneo = None;
            }
            case None => {
              waitingChameneo = Some(chameneo);
            }
          }
        } else if (!sentExit) {
          trigger(Exit -> mallPort);
          this.sentExit = true; // only send once
        } // else just drop, since we already sent exit to everyone
      }
    }
  }

  class ChameneoComponent(init: Init[ChameneoComponent]) extends ComponentDefinition {
    import Messages._;

    val mallPort = requires(MallPort);
    val chameneoPortProv = provides(ChameneoPort);
    val chameneoPortReq = requires(ChameneoPort);

    val Init(initialColour: ChameneosColour) = init;
    val myId = this.id();

    private var colour = initialColour;
    private var meetings: Long = 0L;

    ctrl uponEvent {
      case _: Start => {
        trigger(MeetMe(colour, this.myId) -> mallPort);
      }
    }

    mallPort uponEvent {
      case MeetUp(otherColour, chameneo, this.myId) => {
        colour = colour.complement(otherColour);
        meetings += 1L;
        trigger(Change(colour, chameneo) -> chameneoPortReq);
        trigger(MeetMe(colour, this.myId) -> mallPort);
      }
      case Exit => {
        colour = ChameneosColour.Faded;
        trigger(MeetingCount(meetings) -> mallPort);
        suicide();
      }
    }

    chameneoPortProv uponEvent {
      case Change(newColour, this.myId) => {
        colour = newColour;
        meetings += 1L;
        trigger(MeetMe(colour, this.myId) -> mallPort);
      }
    }
  }

}
