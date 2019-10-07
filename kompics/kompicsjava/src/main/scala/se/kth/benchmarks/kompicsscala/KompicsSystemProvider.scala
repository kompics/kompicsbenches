package se.kth.benchmarks.kompicsscala

import se.kth.benchmarks.BenchmarkException
import se.sics.kompics.{
  Channel,
  Component,
  Fault,
  FaultHandler,
  LoopbackPort,
  PortType,
  ComponentDefinition => JComponentDefinition,
  Init => JInit
}
import se.sics.kompics.config.Conversions
import se.sics.kompics.sl._
import se.sics.kompics.network.{ListeningStatus, Network, NetworkControl, Transport}
import se.sics.kompics.network.netty.{NettyInit, NettyNetwork}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.Duration
import java.util.UUID
import java.net.ServerSocket

import scala.reflect._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import com.google.common.collect.ImmutableSet

import scala.compat.java8.OptionConverters._
import com.typesafe.scalalogging.StrictLogging

case class SetupFH(p: Promise[KompicsSystem]) extends FaultHandler {
  override def handle(f: Fault): Fault.ResolveAction = {
    if (p.isCompleted) { // happened after setup, so we can't use the promise to notify anymore
      return Fault.ResolveAction.ESCALATE; // only option is to quit the JVM
    } else { // happened during setup
      p.failure(f.getCause);
      return Fault.ResolveAction.DESTROY; // can simply tear down Kompics
    }
  }
}

object KompicsSystemProvider extends StrictLogging {

  BenchNet.registerSerializers();
  se.kth.benchmarks.kompicsjava.net.BenchNetSerializer.register();
  Conversions.register(new se.kth.benchmarks.kompicsjava.net.NetAddressConverter());

  val SELF_ADDR_KEY = se.kth.benchmarks.kompics.ConfigKeys.SELF_ADDR_KEY;

  private var publicIf = "127.0.0.1";
  def setPublicIf(pif: String): Unit = this.synchronized { publicIf = pif; };
  def getPublicIf(): String = this.synchronized { publicIf };

  def newKompicsSystem(): KompicsSystem = {
    try {
      val p = Promise[KompicsSystem];
      Kompics.faultHandler = SetupFH(p);
      Kompics.createAndStart(classOf[KompicsSystem], Init(p, None), Runtime.getRuntime.availableProcessors(), 50);
      Await.result(p.future, Duration.Inf)
    } catch {
      case e: Throwable => {
        e.printStackTrace(Console.err);
        Kompics.forceShutdown(); // make sure this failure doesn't affect future tests
        throw e;
      }
    }
  }
  def newKompicsSystem(threads: Int): KompicsSystem = {
    try {
      val p = Promise[KompicsSystem];
      Kompics.faultHandler = SetupFH(p);
      Kompics.createAndStart(classOf[KompicsSystem], Init(p, None), threads, 50);
      Await.result(p.future, Duration.Inf)
    } catch {
      case e: Throwable => {
        e.printStackTrace(Console.err);
        Kompics.forceShutdown(); // make sure this failure doesn't affect future tests
        throw e;
      }
    }
  }
  def newRemoteKompicsSystem(threads: Int): KompicsSystem = {
    try {
      val p = Promise[KompicsSystem];
      val addr = NetAddress.from(getPublicIf(), 0).get;
      logger.info(s"Trying to bind on: $addr");
      val c = Kompics.config.copy(false).asInstanceOf[se.sics.kompics.config.Config.Impl];
      val cb = c.modify(UUID.randomUUID());
      cb.setValue(SELF_ADDR_KEY, addr);
      val cu = cb.finalise();
      c.apply(cu, None);
      Kompics.config = c;
      Kompics.faultHandler = SetupFH(p);
      Kompics.createAndStart(classOf[KompicsSystem], Init(p, Some(addr)), threads, 50);
      Await.result(p.future, Duration.Inf)
    } catch {
      case e: Throwable => {
        e.printStackTrace(Console.err);
        Kompics.forceShutdown(); // make sure this failure doesn't affect future tests
        throw e;
      }
    }
  }
}

case class NewComponent[C <: JComponentDefinition](c: Class[C], init: JInit[C], p: Promise[UUID]) extends KompicsEvent;
case class StartComponent(id: UUID, p: Promise[Unit]) extends KompicsEvent;
case class KillComponent(id: UUID, p: Promise[Unit]) extends KompicsEvent;
case class ConnectComponents[P <: PortType](port: Class[P], requirer: UUID, provider: UUID, p: Promise[Unit])
    extends KompicsEvent;
case class TriggerComponent(id: UUID, event: KompicsEvent, p: Promise[Unit]) extends KompicsEvent;
case class RunOnComponent[T](id: UUID, fun: Component => T, p: Promise[T]) extends KompicsEvent;

class KompicsSystem(init: Init[KompicsSystem]) extends ComponentDefinition {

  private var Init(startPromise: Promise[KompicsSystem] @unchecked, networkAddrO: Option[NetAddress] @unchecked) = init;

  private val children = mutable.TreeMap.empty[UUID, Component];
  private val channels = mutable.TreeMap.empty[UUID, List[Channel[_]]];
  private val awaitingStarted = mutable.TreeMap.empty[UUID, Promise[Unit]];
  private val awaitingKilled = mutable.TreeMap.empty[UUID, Promise[Unit]];

  private val netC = requires[NetworkControl];

  private var networkC: Component = networkAddrO match {
    case Some(addr) => {
      val c = create(classOf[NettyNetwork], new NettyInit(addr, 0, ImmutableSet.of(Transport.TCP)));
      connect[NetworkControl](c -> netC.dualNegative);
      c
    }
    case None => null
  };

  private var (netStarted, netListening) = (false, false);

  def networkAddress: Option[NetAddress] = networkAddrO;

  private def replyPromise() {
    if (netStarted && netListening && (startPromise != null)) {
      startPromise.success(this);
      startPromise = null;
    }
  }

  ctrl uponEvent {
    case _: Start =>
      handle {
        log.info("KompicsSystem started.");
        networkAddrO match {
          case Some(_) => log.debug("Waiting for Network to start...");
          case None => {
            startPromise.success(this);
            startPromise = null;
          }
        }
      }
    case s: Started =>
      handle {
        val cid = s.component.id();
        log.debug(s"Got Started for $cid");
        if ((networkC != null) && (cid == networkC.id())) {
          log.info(s"Network started! (cid = $cid)");
          children += (networkC.id() -> networkC);
          netStarted = true;
          replyPromise();
        } else {
          awaitingStarted.get(cid) match {
            case Some(p) => p.success()
            case None    => log.error(s"Could not find starting component with id=$cid")
          }
        }
      }
    case s: Killed =>
      handle {
        val cid = s.component.id();
        log.debug(s"Got Killed for $cid");
        if ((networkC != null) && (cid == networkC.id())) {
          logger.info("Network component is killed.");
        } else {
          awaitingKilled.remove(cid) match {
            case Some(p) => p.success()
            case None    => log.error(s"Could not find dying component with id=$cid")
          }
        }
      }
  }

  netC uponEvent {
    case status: ListeningStatus =>
      handle {
        val netAddrOJ = status.address(Transport.TCP, isa => NetAddress(isa));
        networkAddrO = netAddrOJ.asScala;
        netListening = true;

        networkAddrO match {
          case Some(addr) => {

            val conf = this.config(); // Java config
            val cb = conf.modify(this.id());
            cb.setValue(KompicsSystemProvider.SELF_ADDR_KEY, addr);
            val cu = cb.finalise();
            updateConfig(cu);
          }
          case None => () // ignore...if the port binding fails the starting should fail, too
        }

        replyPromise();
      }
  }

  loopbck uponEvent {
    case NewComponent(c, i, p) =>
      handle {
        val comp = create(c, i);
        children += (comp.id() -> comp);
        p.success(comp.id());
      }
    case StartComponent(cid, p) =>
      handle {
        children.get(cid) match {
          case Some(c) => {
            logger.debug(s"Sending Start for component ${cid}");
            trigger(Start -> c.control());
            awaitingStarted += (cid -> p);
          }
          case None => p.failure(new BenchmarkException(s"Could not find component with id=$cid to start"))
        }
      }
    case KillComponent(cid, p) =>
      handle {
        channels.remove(cid) match {
          case Some(cs) => cs.foreach(c => c.disconnect())
          case None     => () // ok, no channels connected
        }
        children.get(cid) match {
          case Some(c) => {
            logger.debug(s"Sending Kill for component ${cid}");
            trigger(Kill -> c.control());
            awaitingKilled += (cid -> p);
          }
          case None => p.failure(new BenchmarkException(s"Could not find component with id=$cid to kill"))
        }
      }
    case ConnectComponents(port, req, prov, p) =>
      handle {
        val res = for {
          reqC <- Try(children(req));
          provC <- Try(children(prov))
        } yield {
          val c = connect(reqC.required(port), provC.provided(port), Channel.TWO_WAY);
          val entry1 = channels.getOrElseUpdate(req, List.empty);
          val entry2 = channels.getOrElseUpdate(prov, List.empty);
          channels.put(req, c :: entry1);
          channels.put(prov, c :: entry2);
          ()
        };
        p.complete(res)
      }
    case TriggerComponent(cid, event, p) =>
      handle {
        children.get(cid) match {
          case Some(c) => {
            trigger(event -> c.control())
            p.success()
          }
          case None => p.failure(new BenchmarkException(s"Could not find component with id=$cid to trigger $event on!"))
        }
      }
    case RunOnComponent(cid, fun, p) =>
      handle {
        children.get(cid) match {
          case Some(c) => {
            val res = Try(fun(c));
            p.complete(res)
          }
          case None => p.failure(new BenchmarkException(s"Could not find component with id=$cid to run function on!"))
        }
      }
  }

  //  override def handleFault(fault: Fault): Fault.ResolveAction = {
  //    if (startPromise != null) {
  //      startPromise.failure(fault.getCause);
  //      startPromise = null;
  //    }
  //    logger.error("Top level component caught a fault. Taking down Kompics System!", fault.getCause);
  //    return Fault.ResolveAction.ESCALATE;
  //  }

  def createNotify[C <: JComponentDefinition: ClassTag](init: JInit[C]): Future[UUID] = {
    val ct = classTag[C].runtimeClass.asInstanceOf[Class[C]];
    val p = Promise[UUID];
    trigger(NewComponent(ct, init, p) -> onSelf);
    p.future
  }

  def startNotify(id: UUID): Future[Unit] = {
    val p = Promise[Unit];
    trigger(StartComponent(id, p) -> onSelf);
    p.future
  }

  def killNotify(id: UUID): Future[Unit] = {
    val p = Promise[Unit];
    trigger(KillComponent(id, p) -> onSelf);
    p.future
  }

  def connectComponents[P <: PortType: ClassTag](requirer: UUID, provider: UUID): Future[Unit] = {
    val p = Promise[Unit];
    val port = classTag[P].runtimeClass.asInstanceOf[Class[P]];
    assert(!port.getName.equals("scala.runtime.Nothing$"));
    trigger(ConnectComponents(port, requirer, provider, p) -> onSelf);
    p.future
  }

  def connectNetwork(requirer: UUID): Future[Unit] = {
    val idRes = Try(networkC.id()); // could null and if it is, I want the exception
    idRes match {
      case Success(provider) => connectComponents[Network](requirer, provider)
      case Failure(e)        => Future.failed(e)
    }
  }

  def triggerComponent(event: KompicsEvent, id: UUID): Future[Unit] = {
    val p = Promise[Unit];
    trigger(TriggerComponent(id, event, p) -> onSelf)
    p.future
  }

  def runOnComponent[T](id: UUID)(f: Component => T): Future[T] = {
    val p = Promise[T];
    trigger(RunOnComponent(id, f, p) -> onSelf)
    p.future
  }

  def terminate(): Unit = Kompics.shutdown();
}
