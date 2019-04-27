package se.kth.benchmarks.kompicsscala

import se.kth.benchmarks.BenchmarkException
import se.sics.kompics.{ Start, Started, Kill, Killed, Kompics, KompicsEvent, Component, Init => JInit, PortType, Channel }
import se.sics.kompics.sl._
import se.sics.kompics.network.{ Transport, Network }
import se.sics.kompics.network.netty.{ NettyNetwork, NettyInit }
import scala.concurrent.{ Future, Promise, Await }
import scala.concurrent.duration.Duration
import java.util.UUID
import java.net.ServerSocket;
import scala.reflect._
import scala.collection.mutable
import scala.util.{ Failure, Success, Try }
import com.google.common.collect.ImmutableSet;

object KompicsSystemProvider {

  BenchNet.registerSerializers();

  val SELF_ADDR_KEY = "self-address";

  private var publicIf = "127.0.0.1";
  def setPublicIf(pif: String): Unit = this.synchronized{ publicIf = pif; };
  def getPublicIf(): String = this.synchronized { publicIf };

  def newKompicsSystem(): KompicsSystem = {
    val p = Promise[KompicsSystem];
    Kompics.createAndStart(classOf[KompicsSystem], Init(p, None), Runtime.getRuntime.availableProcessors(), 50);
    val res = Await.result(p.future, Duration.Inf);
    res
  }
  def newKompicsSystem(threads: Int): KompicsSystem = {
    val p = Promise[KompicsSystem];
    Kompics.createAndStart(classOf[KompicsSystem], Init(p, None), threads, 50);
    val res = Await.result(p.future, Duration.Inf);
    res
  }
  def newRemoteKompicsSystem(threads: Int): KompicsSystem = {
    val p = Promise[KompicsSystem];
    var s: ServerSocket = null;
    try {
      s = new ServerSocket(0);
      val addr = NetAddress.from(getPublicIf(), s.getLocalPort).get;
      println(s"Trying to bind on: $addr");
      val c = Kompics.getConfig().copy(false).asInstanceOf[se.sics.kompics.config.Config.Impl];
      val cb = c.modify(UUID.randomUUID());
      cb.setValue(SELF_ADDR_KEY, addr);
      val cu = cb.finalise();
      c.apply(cu, com.google.common.base.Optional.absent());
      Kompics.setConfig(c);
      Kompics.createAndStart(classOf[KompicsSystem], Init(p, Some(addr)), threads, 50);
      val res = Await.result(p.future, Duration.Inf);
      return res;
    } finally {
      if (s != null) {
        s.close();
      }
    }
  }
}

case class NewComponent[C <: ComponentDefinition](c: Class[C], init: JInit[C], p: Promise[UUID]) extends KompicsEvent;
case class StartComponent(id: UUID, p: Promise[Unit]) extends KompicsEvent;
case class KillComponent(id: UUID, p: Promise[Unit]) extends KompicsEvent;
case class ConnectComponents[P <: PortType](port: Class[P], requirer: UUID, provider: UUID, p: Promise[Unit]) extends KompicsEvent;

class KompicsSystem(init: Init[KompicsSystem]) extends ComponentDefinition {

  private val Init(startPromise: Promise[KompicsSystem] @unchecked, networkAddrO: Option[NetAddress] @unchecked) = init;

  private val children = mutable.TreeMap.empty[UUID, Component];
  private val awaitingStarted = mutable.TreeMap.empty[UUID, Promise[Unit]];
  private val awaitingKilled = mutable.TreeMap.empty[UUID, Promise[Unit]];

  private var networkC: Component = networkAddrO match {
    case Some(addr) => create(classOf[NettyNetwork], new NettyInit(addr, 0, ImmutableSet.of(Transport.TCP)))
    case None       => null
  };

  def networkAddress: Option[NetAddress] = networkAddrO;

  ctrl uponEvent {
    case _: Start => handle {
      log.info("KompicsSystem started.");
      networkAddrO match {
        case Some(_) => log.debug("Waiting for Network to start...");
        case None    => startPromise.success(this);
      }
    }
    case s: Started => handle {
      val cid = s.component.id();
      log.debug(s"Got Started for $cid");
      if ((networkC != null) && (cid == networkC.id())) {
        log.info(s"Network started!");
        children += (networkC.id() -> networkC);
        startPromise.success(this);
      } else {
        awaitingStarted.get(cid) match {
          case Some(p) => p.success()
          case None    => log.error(s"Could not find component with id=$cid")
        }
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

  def connectNetwork(requirer: UUID): Future[Unit] = {
    val idRes = Try(networkC.id()); // could null and if it is, I want the exception
    idRes match {
      case Success(provider) => connectComponents[Network](requirer, provider)
      case Failure(e)        => Future.failed(e)
    }
  }

  def terminate(): Unit = Kompics.shutdown();
}
