package se.kth.benchmarks.kompicsscala.bench

import org.scalatest._
import scala.util.{ Success, Failure, Try }
import se.kth.benchmarks.kompicsscala._
import java.util.concurrent.CountDownLatch
import se.sics.kompics.sl._
import scala.concurrent.Await
import scala.concurrent.duration._

class DistributedTest extends FunSuite with Matchers {

  implicit val ec = scala.concurrent.ExecutionContext.global;

  test("Network Ser/Deser") {
    import NetPingPong._
    import se.sics.kompics.network.netty.serialization.Serializers;
    import io.netty.buffer.{ Unpooled, ByteBuf };
    import java.util.Optional;

    BenchNet.registerSerializers();
    NetPingPongSerializer.register();

    val noHint: Optional[Object] = Optional.empty();

    val addr = NetAddress.from("127.0.0.1", 12345).get;
    val addr2 = NetAddress.from("127.0.0.1", 45678).get; // larger port number that doesn't fit into a short
    val ping = NetMessage.viaTCP(addr, addr)(Ping);
    val pong = NetMessage.viaTCP(addr, addr)(Pong);

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
    pingDeserO shouldBe a[NetMessage[_]];
    val pingDeser = pingDeserO.asInstanceOf[NetMessage[Ping.type]];
    pingDeser should equal (ping);

    buf.clear();

    Serializers.toBinary(pong, buf);
    val pongDeserO = Serializers.fromBinary(buf, noHint);
    pongDeserO shouldBe a[NetMessage[_]];
    val pongDeser = pongDeserO.asInstanceOf[NetMessage[Pong.type]];
    pongDeser should equal (pong);
  }

  test("Throughput Network Ser/Deser") {
    import NetThroughputPingPong._
    import se.sics.kompics.network.netty.serialization.Serializers;
    import io.netty.buffer.{ Unpooled, ByteBuf };
    import java.util.Optional;

    BenchNet.registerSerializers();
    NetPingPongSerializer.register();

    val noHint: Optional[Object] = Optional.empty();

    val addr = NetAddress.from("127.0.0.1", 12345).get;
    val addr2 = NetAddress.from("127.0.0.1", 45678).get; // larger port number that doesn't fit into a short
    val sping = NetMessage.viaTCP(addr, addr)(StaticPing(1));
    val spong = NetMessage.viaTCP(addr, addr)(StaticPong(1));
    val ping = NetMessage.viaTCP(addr, addr)(Ping(42, 1));
    val pong = NetMessage.viaTCP(addr, addr)(Pong(42, 1));

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
    spingDeserO shouldBe a[NetMessage[_]];
    val spingDeser = spingDeserO.asInstanceOf[NetMessage[StaticPing]];
    spingDeser should equal (sping);

    buf.clear();

    Serializers.toBinary(spong, buf);
    val spongDeserO = Serializers.fromBinary(buf, noHint);
    spongDeserO shouldBe a[NetMessage[_]];
    val spongDeser = spongDeserO.asInstanceOf[NetMessage[StaticPong]];
    spongDeser should equal (spong);

    buf.clear();

    Serializers.toBinary(ping, buf);
    val pingDeserO = Serializers.fromBinary(buf, noHint);
    pingDeserO shouldBe a[NetMessage[_]];
    val pingDeser = pingDeserO.asInstanceOf[NetMessage[Ping]];
    pingDeser should equal (ping);

    buf.clear();

    Serializers.toBinary(pong, buf);
    val pongDeserO = Serializers.fromBinary(buf, noHint);
    pongDeserO shouldBe a[NetMessage[_]];
    val pongDeser = pongDeserO.asInstanceOf[NetMessage[Pong]];
    pongDeser should equal (pong);
  }

  test("NetPingPong Ser/Deser") {
    val addr = NetAddress.from("127.0.0.1", 12345).get;
    println(s"Original Address: $addr");
    val ser = NetPingPong.clientDataToString(addr);
    println(s"Serialised Address: $ser");
    val deser = NetPingPong.strToClientData(ser).get;
    println(s"Deserialised Address: $deser");
    deser.getIp() should equal (addr.getIp());
    deser.getPort() should equal (addr.getPort());
    deser should equal (addr);
  }

  test("NetThroughputPingPong Ser/Deser") {
    val addr = NetAddress.from("127.0.0.1", 12345).get;
    println(s"Original Address: $addr");
    val ser = NetThroughputPingPong.clientDataToString(addr);
    println(s"Serialised Address: $ser");
    val deser = NetThroughputPingPong.strToClientData(ser).get;
    println(s"Deserialised Address: $deser");
    deser.getIp() should equal (addr.getIp());
    deser.getPort() should equal (addr.getPort());
    deser should equal (addr);
  }

  test("Kompics Scala Network System") {
    import NetPingPong._;
    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[Pinger](Init(latch, 100l, addr));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    println(s"Pinger Path is $addr");

    val pongerF = system.createNotify[Ponger](Init.none);
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

  test("Kompics Scala Throughput Network System (Static)") {
    import NetThroughputPingPong._;
    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[StaticPinger](Init(1, latch, 100l, 2l, addr));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    println(s"Pinger Path is $addr");

    val pongerF = system.createNotify[StaticPonger](Init(1));
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

  test("Kompics Scala Throughput Network System (GC)") {
    import NetThroughputPingPong._;
    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[Pinger](Init(1, latch, 100l, 2l, addr));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    println(s"Pinger Path is $addr");

    val pongerF = system.createNotify[Ponger](Init(1));
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
