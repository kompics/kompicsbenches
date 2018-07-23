package se.kth.benchmarks

import se.sics.kompics.{ Start, Started, Kill, Killed, Kompics, KompicsEvent, Component, Init => JInit, PortType, Channel }
import se.sics.kompics.sl._
import scala.concurrent.{ Future, Promise, Await }
import scala.concurrent.duration.Duration
import java.util.UUID
import scala.reflect._
import scala.collection.mutable
import scala.util.Try

object KompicsSystemProvider {
  def newKompicsSystem(): KompicsSystem = {
    val p = Promise[KompicsSystem];
    Kompics.createAndStart(classOf[KompicsSystem], Init(p), Runtime.getRuntime.availableProcessors(), 50);
    val res = Await.result(p.future, Duration.Inf);
    res
  }
  def newKompicsSystem(threads: Int): KompicsSystem = {
    val p = Promise[KompicsSystem];
    Kompics.createAndStart(classOf[KompicsSystem], Init(p), threads, 50);
    val res = Await.result(p.future, Duration.Inf);
    res
  }
}

case class NewComponent[C <: ComponentDefinition](c: Class[C], init: JInit[C], p: Promise[UUID]) extends KompicsEvent;
case class StartComponent(id: UUID, p: Promise[Unit]) extends KompicsEvent;
case class KillComponent(id: UUID, p: Promise[Unit]) extends KompicsEvent;
case class ConnectComponents[P <: PortType](port: Class[P], requirer: UUID, provider: UUID, p: Promise[Unit]) extends KompicsEvent;

class KompicsSystem(init: Init[KompicsSystem]) extends ComponentDefinition {
  init match {
    case Init(p: Promise[KompicsSystem] @unchecked) => p.success(this)
  }

  private val children = mutable.TreeMap.empty[UUID, Component];
  private val awaitingStarted = mutable.TreeMap.empty[UUID, Promise[Unit]];
  private val awaitingKilled = mutable.TreeMap.empty[UUID, Promise[Unit]];

  ctrl uponEvent {
    case _: Start => handle {
      log.info("KompicsSystem started.");
    }
    case s: Started => handle {
      val cid = s.component.id();
      log.debug(s"Got Started for $cid");
      awaitingStarted.get(cid) match {
        case Some(p) => p.success()
        case None    => log.error(s"Could not find component with id=$cid")
      }
    }
    case s: Killed => handle {
      val cid = s.component.id();
      log.debug(s"Got Killed for $cid");
      awaitingKilled.remove(cid) match {
        case Some(p) => p.success()
        case None    => log.error(s"Could not find component with id=$cid")
      }
    }
  }

  loopbck uponEvent {
    case NewComponent(c, i, p) => handle {
      val comp = create(c, i);
      children += (comp.id() -> comp);
      p.success(comp.id());
    }
    case StartComponent(cid, p) => handle {
      children.get(cid) match {
        case Some(c) => {
          logger.debug(s"Sending Start for component ${cid}");
          trigger(Start.event -> c.control());
          awaitingStarted += (cid -> p);
        }
        case None => p.failure(new BenchmarkException(s"Could not find component with id=$cid"))
      }
    }
    case KillComponent(cid, p) => handle {
      children.get(cid) match {
        case Some(c) => {
          logger.debug(s"Sending Kill for component ${cid}");
          trigger(Kill.event -> c.control());
          awaitingKilled += (cid -> p);
        }
        case None => p.failure(new BenchmarkException(s"Could not find component with id=$cid"))
      }
    }
    case ConnectComponents(port, req, prov, p) => handle {
      val res = for {
        reqC <- Try(children(req));
        provC <- Try(children(prov))
      } yield {
        connect(reqC.required(port), provC.provided(port), Channel.TWO_WAY);
        ()
      };
      p.complete(res)
    }
  }

  def createNotify[C <: ComponentDefinition: ClassTag](init: JInit[C]): Future[UUID] = {
    val ct = classTag[C].runtimeClass.asInstanceOf[Class[C]];
    val p = Promise[UUID];
    trigger (NewComponent(ct, init, p) -> onSelf);
    p.future
  }

  def startNotify(id: UUID): Future[Unit] = {
    val p = Promise[Unit];
    trigger (StartComponent(id, p) -> onSelf);
    p.future
  }

  def killNotify(id: UUID): Future[Unit] = {
    val p = Promise[Unit];
    trigger (KillComponent(id, p) -> onSelf);
    p.future
  }

  def connectComponents[P <: PortType: ClassTag](requirer: UUID, provider: UUID): Future[Unit] = {
    val p = Promise[Unit];
    val port = classTag[P].runtimeClass.asInstanceOf[Class[P]];
    assert(!port.getName.equals("scala.runtime.Nothing$"));
    trigger (ConnectComponents(port, requirer, provider, p) -> onSelf);
    p.future
  }

  def terminate(): Unit = Kompics.shutdown();
}
