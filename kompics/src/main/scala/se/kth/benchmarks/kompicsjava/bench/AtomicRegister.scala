package se.kth.benchmarks.kompicsjava.bench

import java.util.{ HashSet, UUID }
import java.util.concurrent.CountDownLatch

import kompics.benchmarks.benchmarks.AtomicRegisterRequest
import se.kth.benchmarks.{ ClientEntry, DistributedBenchmark }
import se.kth.benchmarks.kompicsjava.broadcast.{ BEBComp, BestEffortBroadcast => JBestEffortBroadcast }
import se.kth.benchmarks.kompicsjava.bench.atomicregister.{ AtomicRegister, AtomicRegisterSerializer }
import se.kth.benchmarks.kompicsjava.net.{ NetAddress => JNetAddress }
import se.kth.benchmarks.kompicsscala._
import se.sics.kompics.sl.Init
import se.sics.kompics.timer.Timer
import se.sics.kompics.timer.java.JavaTimer

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{ Success, Try }

object AtomicRegister extends DistributedBenchmark {
  override type MasterConf = AtomicRegisterRequest;
  override type ClientConf = AtomicRegisterRequest;
  override type ClientData = NetAddress;

  class MasterImpl extends Master {
    private var num_reads = -1l;
    private var num_writes = -1l;
    private var system: KompicsSystem = null;
    private var atomicRegister: UUID = null;
    private var beb: UUID = null;
    private var timer: UUID = null;
    private var latch: CountDownLatch = null;
    private var init_id: Int = 0;

    override def setup(c: MasterConf): ClientConf = {
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      AtomicRegisterSerializer.register();
      this.num_reads = c.numberOfReads;
      this.num_writes = c.numberOfWrites;
      println(s"Atomic Register(Master) setup: numReads=$num_reads numWrites=$num_writes")
      c
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      assert(system != null);
      latch = new CountDownLatch(1);
      val addr = system.networkAddress.get;
      println(s"Atomic Register(Master) Path is $addr");

      init_id -= 1
      var nodes = new java.util.HashSet[JNetAddress]()
      nodes.add(addr.asJava)
      for (node <- d) { nodes.add(node.asJava) }
      val atomicRegisterIdF = system.createNotify[AtomicRegister](new atomicregister.AtomicRegister.Init(latch, nodes, num_reads, num_writes, init_id))
      atomicRegister = Await.result(atomicRegisterIdF, 5.second);
      /* connect network */
      val connF = system.connectNetwork(atomicRegister);
      Await.result(connF, 5.seconds);
      /* connect best effort broadcast */
      val bebF = system.createNotify[BEBComp](new BEBComp.Init(addr.asJava))
      beb = Await.result(bebF, 5.second)
      val beb_net_connF = system.connectNetwork(beb)
      Await.result(beb_net_connF, 5.second)
      val beb_ar_connF = system.connectComponents[JBestEffortBroadcast](atomicRegister, beb)
      Await.result(beb_ar_connF, 5.seconds);
      /* connect timer */
      val timerF = system.createNotify[JavaTimer](Init.none[JavaTimer])
      timer = Await.result(timerF, 5.second)
      val timer_connF = system.connectComponents[Timer](atomicRegister, timer)
      Await.result(timer_connF, 5.seconds);
    }
    override def runIteration(): Unit = {
      assert(system != null && atomicRegister != null && beb != null && timer != null);
      println("run iteration!")
      system.startNotify(timer)
      system.startNotify(beb)
      val startF = system.startNotify(atomicRegister);
      //startF.failed.foreach(e => eprintln(s"Could not start pinger: $e"));
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up Atomic Register(Master) side");
      assert(system != null);
      if (latch != null) {
        latch = null;
      }
      if (atomicRegister != null) {
        val killF = system.killNotify(atomicRegister);
        Await.ready(killF, 5.seconds);
        val killBebF = system.killNotify(beb)
        Await.ready(killBebF, 5.seconds);
        val killTimerF = system.killNotify(timer)
        Await.ready(killTimerF, 5.seconds)
        atomicRegister = null;
        beb = null
        timer = null
      }
      if (lastIteration) {
        val f = system.terminate();
        system = null;
      }
    }
  }

  class ClientImpl extends Client {
    private var num_reads = -1l;
    private var num_writes = -1l;
    private var system: KompicsSystem = null;
    private var atomicRegister: UUID = null;
    private var beb: UUID = null;
    private var latch: CountDownLatch = null

    override def setup(c: ClientConf): ClientData = {
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      latch = new CountDownLatch(1);
      AtomicRegisterSerializer.register();
      val addr = system.networkAddress.get;
      println(s"Atomic Register(Client) Path is $addr");
      /* Atomic Register */
      this.num_reads = c.numberOfReads;
      this.num_writes = c.numberOfWrites;
      val atomicRegisterIdF = system.createNotify[AtomicRegister](new atomicregister.AtomicRegister.Init(latch, null, num_reads, num_writes, 0))
      atomicRegister = Await.result(atomicRegisterIdF, 5.second);
      /* connect network */
      val connF = system.connectNetwork(atomicRegister);
      Await.result(connF, 5.seconds);
      /* connect best effort broadcast */
      val bebF = system.createNotify[BEBComp](new BEBComp.Init(addr.asJava)) // TODO: is addr correct?
      beb = Await.result(bebF, 5.second)
      val beb_net_connF = system.connectNetwork(beb)
      Await.result(beb_net_connF, 5.second)
      val beb_ar_connF = system.connectComponents[JBestEffortBroadcast](atomicRegister, beb)
      Await.result(beb_ar_connF, 5.seconds);
      system.startNotify(beb)
      system.startNotify(atomicRegister)
      addr
    }
    override def prepareIteration(): Unit = {
      // nothing
      println("Preparing Atomic Register(Client) iteration");
      assert(system != null);

    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      println("Cleaning up Atomic Register(Client) side");
      assert(system != null);
      if (lastIteration) {
        atomicRegister = null; // will be stopped when system is shut down
        beb = null
        system.terminate();
        system = null;
      }
    }
  }

  override def newMaster(): AtomicRegister.Master = new MasterImpl();

  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[AtomicRegisterRequest]
  };

  override def newClient(): AtomicRegister.Client = new ClientImpl();

  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(":");
    assert(split.length == 2);
    AtomicRegisterRequest({ split(0).toInt }, { split(1).toInt })
  };

  override def strToClientData(str: String): Try[ClientData] = NetAddress.fromString(str);

  override def clientConfToString(c: ClientConf): String = s"${c.numberOfReads}:${c.numberOfWrites}";

  override def clientDataToString(d: ClientData): String = s"${d.isa.getHostString()}:${d.getPort()}";

}
