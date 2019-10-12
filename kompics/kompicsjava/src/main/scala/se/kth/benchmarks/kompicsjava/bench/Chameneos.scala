package se.kth.benchmarks.kompicsjava.bench

import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider}
import se.kth.benchmarks.Benchmark
import kompics.benchmarks.benchmarks.ChameneosRequest
import se.sics.kompics.{Component, Fault, Kill, Killed, Kompics, KompicsEvent, Start, Started}
import se.sics.kompics.sl._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Try
import java.util.concurrent.CountDownLatch
import com.typesafe.scalalogging.StrictLogging
import java.util.UUID
import se.kth.benchmarks.kompicsjava.bench.chameneos._

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
          new ChameneosMallComponent.Init(numMeetings, numChameneos, latch)
        );
        chameneosIds <- Future.sequence((for (i <- 0 until this.numChameneos) yield {
          val initialColour = ChameneosColour.forId(i);
          system.createNotify[ChameneoComponent](new ChameneoComponent.Init(initialColour))
        }).toList);
        _ <- Future.sequence(chameneosIds.flatMap(cid => {
          val mallConnF = system.connectComponents[MallPort](cid, mallId);
          val subConnF: List[Future[Unit]] = chameneosIds.map(cid2 => {
            if (cid != cid2) {
              system.connectComponents[ChameneoPort](cid2, cid);
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

}
