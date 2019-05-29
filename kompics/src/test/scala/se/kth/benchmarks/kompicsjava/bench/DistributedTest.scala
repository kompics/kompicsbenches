package se.kth.benchmarks.kompicsjava.bench

import org.scalatest._

import scala.util.{ Failure, Success, Try }
import se.kth.benchmarks.kompicsscala.{ KompicsSystemProvider, NetAddress => SNetAddress }
import se.kth.benchmarks.kompicsjava._
import se.kth.benchmarks.kompicsjava.net._
import java.util.concurrent.CountDownLatch

import NetPingPong._
import se.kth.benchmarks.kompicsjava.bench.atomicregister.events._;
import se.sics.kompics.sl._

import scala.concurrent.Await
import scala.concurrent.duration._

class DistributedTest extends FunSuite with Matchers {

  implicit val ec = scala.concurrent.ExecutionContext.global;

  test("Network Ser/Deser") {

    import se.kth.benchmarks.kompicsjava.bench.netpingpong._
    import se.kth.benchmarks.kompicsjava.bench.atomicregister._
    import se.kth.benchmarks.kompicsjava.bench.atomicregister.ReadImposeWriteConsultMajority
    import se.sics.kompics.network.netty.serialization.Serializers;
    import io.netty.buffer.{ Unpooled, ByteBuf };
    import java.util.Optional;

    BenchNetSerializer.register();
    NetPingPongSerializer.register();
    AtomicRegisterSerializer.register();

    val noHint: Optional[Object] = Optional.empty();

    val addr = NetAddress.from("127.0.0.1", 12345);
    val addr2 = NetAddress.from("127.0.0.1", 45678); // larger port number that doesn't fit into a short
    val ping = NetMessage.viaTCP(addr, addr, Ping.event);
    val pong = NetMessage.viaTCP(addr, addr, Pong.event);

    // Atomic Register events
    val rid = 123;
    val ts = 1;
    val wr = 2;
    val v = 3;
    val read = NetMessage.viaTCP(addr, addr, new READ(rid));
    val ack = NetMessage.viaTCP(addr, addr, new ACK(rid));
    val write = NetMessage.viaTCP(addr, addr, new WRITE(rid, ts, wr, v));
    val value = NetMessage.viaTCP(addr, addr, new VALUE(rid, ts, wr, v));

    val buf = Unpooled.directBuffer();

    Serializers.toBinary(addr, buf);
    val addrDeserO = Serializers.fromBinary(buf, noHint);
    addrDeserO shouldBe a[NetAddress];
    val addrDeser = addrDeserO.asInstanceOf[NetAddress];
    addrDeser should equal (addr);

    buf.clear();

    Serializers.toBinary(addr2, buf);
    val addr2DeserO = Serializers.fromBinary(buf, noHint);
    addr2DeserO shouldBe a[NetAddress];
    val addr2Deser = addr2DeserO.asInstanceOf[NetAddress];
    addr2Deser should equal (addr2);

    buf.clear();

    Serializers.toBinary(ping, buf);
    val pingDeserO = Serializers.fromBinary(buf, noHint);
    pingDeserO shouldBe a[NetMessage];
    val pingDeser = pingDeserO.asInstanceOf[NetMessage];
    pingDeser should equal (ping);

    buf.clear();

    Serializers.toBinary(pong, buf);
    val pongDeserO = Serializers.fromBinary(buf, noHint);
    pongDeserO shouldBe a[NetMessage];
    val pongDeser = pongDeserO.asInstanceOf[NetMessage];
    pongDeser should equal (pong);

    Serializers.toBinary(read, buf);
    val readDeserO = Serializers.fromBinary(buf, noHint);
    readDeserO shouldBe a[NetMessage];
    val readDeser = readDeserO.asInstanceOf[NetMessage];
    readDeser should equal (read);

    buf.clear()

    Serializers.toBinary(ack, buf);
    val ackDeserO = Serializers.fromBinary(buf, noHint);
    ackDeserO shouldBe a[NetMessage];
    val ackDeser = ackDeserO.asInstanceOf[NetMessage];
    ackDeser should equal (ack);

    buf.clear()

    Serializers.toBinary(write, buf);
    val writeDeserO = Serializers.fromBinary(buf, noHint);
    writeDeserO shouldBe a[NetMessage];
    val writeDeser = writeDeserO.asInstanceOf[NetMessage];
    writeDeser should equal (write);

    buf.clear()

    Serializers.toBinary(value, buf);
    val valueDeserO = Serializers.fromBinary(buf, noHint);
    valueDeserO shouldBe a[NetMessage];
    val valueDeser = valueDeserO.asInstanceOf[NetMessage];
    valueDeser should equal (value);

    buf.clear()
  }

  test("Throughput Network Ser/Deser") {

    import se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong._
    import se.sics.kompics.network.netty.serialization.Serializers;
    import io.netty.buffer.{ Unpooled, ByteBuf };
    import java.util.Optional;

    BenchNetSerializer.register();
    NetPingPongSerializer.register();

    val noHint: Optional[Object] = Optional.empty();

    val addr = NetAddress.from("127.0.0.1", 12345);
    val addr2 = NetAddress.from("127.0.0.1", 45678); // larger port number that doesn't fit into a short
    val sping = NetMessage.viaTCP(addr, addr, StaticPing.event(1));
    val spong = NetMessage.viaTCP(addr, addr, StaticPong.event(1));
    val ping = NetMessage.viaTCP(addr, addr, new Ping(42, 1));
    val pong = NetMessage.viaTCP(addr, addr, new Pong(42, 1));

    val buf = Unpooled.directBuffer();

    Serializers.toBinary(addr, buf);
    val addrDeserO = Serializers.fromBinary(buf, noHint);
    addrDeserO shouldBe a[NetAddress];
    val addrDeser = addrDeserO.asInstanceOf[NetAddress];
    addrDeser should equal (addr);

    buf.clear();

    Serializers.toBinary(addr2, buf);
    val addr2DeserO = Serializers.fromBinary(buf, noHint);
    addr2DeserO shouldBe a[NetAddress];
    val addr2Deser = addr2DeserO.asInstanceOf[NetAddress];
    addr2Deser should equal (addr2);

    buf.clear();

    Serializers.toBinary(sping, buf);
    val spingDeserO = Serializers.fromBinary(buf, noHint);
    spingDeserO shouldBe a[NetMessage];
    val spingDeser = spingDeserO.asInstanceOf[NetMessage];
    spingDeser should equal (sping);

    buf.clear();

    Serializers.toBinary(spong, buf);
    val spongDeserO = Serializers.fromBinary(buf, noHint);
    spongDeserO shouldBe a[NetMessage];
    val spongDeser = spongDeserO.asInstanceOf[NetMessage];
    spongDeser should equal (spong);

    buf.clear();

    Serializers.toBinary(ping, buf);
    val pingDeserO = Serializers.fromBinary(buf, noHint);
    pingDeserO shouldBe a[NetMessage];
    val pingDeser = pingDeserO.asInstanceOf[NetMessage];
    pingDeser should equal (ping);

    buf.clear();

    Serializers.toBinary(pong, buf);
    val pongDeserO = Serializers.fromBinary(buf, noHint);
    pongDeserO shouldBe a[NetMessage];
    val pongDeser = pongDeserO.asInstanceOf[NetMessage];
    pongDeser should equal (pong);
  }

  test("Address Ser/Deser") {
    val addr = SNetAddress.from("127.0.0.1", 12345).get;
    println(s"Original Address: $addr");
    val ser = NetPingPong.clientDataToString(addr);
    println(s"Serialised Address: $ser");
    val deser = NetPingPong.strToClientData(ser).get;
    println(s"Deserialised Address: $deser");
    deser.getIp() should equal (addr.getIp());
    deser.getPort() should equal (addr.getPort());
    deser should equal (addr);
  }

  test("Kompics Java Network System") {
    import se.kth.benchmarks.kompicsjava.bench.netpingpong._

    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[Pinger](new Pinger.Init(latch, 100l, addr.asJava));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    println(s"Pinger Path is $addr");

    val pongerF = system.createNotify[Ponger](Init.none[Ponger]);
    val ponger = Await.result(pongerF, 5.seconds);
    val pongerConnF = system.connectNetwork(ponger);
    Await.result(pongerConnF, 5.seconds);
    println(s"Ponger Path is $addr");
    val pongerStartF = system.startNotify(ponger);
    Await.result(pongerStartF, 5.seconds);
    //pongerStartF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));

    val pingerStartF = system.startNotify(pinger);
    //startF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));
    Await.result(pingerStartF, 5.seconds);

    println("Awaiting test result");
    latch.await();

    system.terminate();
  }

  test("Kompics Java Throughput Network System (GC)") {
    import se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong._

    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[Pinger](new Pinger.Init(1, latch, 100l, 2, addr.asJava));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    println(s"Pinger Path is $addr");

    val pongerF = system.createNotify[Ponger](new Ponger.Init(1));
    val ponger = Await.result(pongerF, 5.seconds);
    val pongerConnF = system.connectNetwork(ponger);
    Await.result(pongerConnF, 5.seconds);
    println(s"Ponger Path is $addr");
    val pongerStartF = system.startNotify(ponger);
    Await.result(pongerStartF, 5.seconds);
    //pongerStartF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));

    val pingerStartF = system.startNotify(pinger);
    //startF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));
    Await.result(pingerStartF, 5.seconds);

    println("Awaiting test result");
    latch.await();

    system.terminate();
  }

  test("Kompics Java Throughput Network System (Static)") {
    import se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong._

    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[StaticPinger](new StaticPinger.Init(1, latch, 100l, 2, addr.asJava));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    println(s"Pinger Path is $addr");

    val pongerF = system.createNotify[StaticPonger](new StaticPonger.Init(1));
    val ponger = Await.result(pongerF, 5.seconds);
    val pongerConnF = system.connectNetwork(ponger);
    Await.result(pongerConnF, 5.seconds);
    println(s"Ponger Path is $addr");
    val pongerStartF = system.startNotify(ponger);
    Await.result(pongerStartF, 5.seconds);
    //pongerStartF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));

    val pingerStartF = system.startNotify(pinger);
    //startF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));
    Await.result(pingerStartF, 5.seconds);

    println("Awaiting test result");
    latch.await();

    system.terminate();
  }
}
