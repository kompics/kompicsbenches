package se.kth.benchmarks.kompicsjava.bench

import java.util.UUID
import java.util.concurrent.CountDownLatch

import kompics.benchmarks.benchmarks.PingPongRequest
import scalapb.GeneratedMessage
import se.kth.benchmarks.DistributedBenchmark
import se.kth.benchmarks.kompicsjava.bench.NetPingPong.{ClientConf, ClientData, MasterConf}
import se.kth.benchmarks.kompicsjava.bench.atomicregister.{AtomicRegisterSerializer, ReadImposeWriteConsultMajority}
import se.kth.benchmarks.kompicsjava.bench.netpingpong.{NetPingPongSerializer, Pinger}
import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider, NetAddress}

import scala.concurrent.Await
import scala.util.Try

object AtomicRegister extends DistributedBenchmark {
  override type MasterConf = PingPongRequest; // TODO; AtomicRegisterRequest?
  override type ClientConf = Unit;
  override type ClientData = NetAddress;

  class MasterImpl extends Master {
    private var num_reads = -1l;
    private var num_writes = -1l;
    private var system: KompicsSystem = null;
    private var pinger: UUID = null;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf): ClientConf = {
      this.num_reads = c.numberOfMessages;
      this.num_writes = c.numberOfMessages;
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      AtomicRegisterSerializer.register();
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      assert(system != null);
      val selfRank = 0;
      val selfAddr = d.head;
      latch = new CountDownLatch(1);
      val atomicRegisterIdF = system.createNotify[ReadImposeWriteConsultMajority](new ReadImposeWriteConsultMajority.Init(selfRank, selfAddr, d));
      val pingerIdF = system.createNotify[Pinger](new Pinger.Init(latch, num, pongerAddr.asJava));
      pinger = Await.result(pingerIdF, 5.second);
      val connF = system.connectNetwork(pinger);
      Await.result(connF, 5.seconds);
      val addr = system.networkAddress.get;
      println(s"Pinger Path is $addr");
    }
    override def runIteration(): Unit = {
      assert(system != null);
      assert(pinger != null);
      val startF = system.startNotify(pinger);
      //startF.failed.foreach(e => eprintln(s"Could not start pinger: $e"));
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up pinger side");
      assert(system != null);
      if (latch != null) {
        latch = null;
      }
      if (pinger != null) {
        val killF = system.killNotify(pinger);
        Await.ready(killF, 5.seconds);
        pinger = null;
      }
      if (lastIteration) {
        val f = system.terminate();
        system = null;
      }
    }
  }


  override def newMaster(): AtomicRegister.Master = ???

  override def msgToMasterConf(msg: GeneratedMessage): Try[AtomicRegister.type] = ???

  override def newClient(): AtomicRegister.Client = ???

  override def strToClientConf(str: String): Try[AtomicRegister.type] = ???

  override def strToClientData(str: String): Try[AtomicRegister.type] = ???

  override def clientConfToString(c: AtomicRegister): String = ???

  override def clientDataToString(d: AtomicRegister): String = ???
}
