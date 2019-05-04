package se.kth.benchmarks.kompicsscala.bench

import org.scalatest._
import scala.util.{ Success, Failure, Try }
import se.kth.benchmarks.kompicsscala._
import java.util.concurrent.CountDownLatch
import NetPingPong._
import se.sics.kompics.sl._
import scala.concurrent.Await
import scala.concurrent.duration._

class DistributedTest extends FunSuite with Matchers {

  implicit val ec = scala.concurrent.ExecutionContext.global;

  test("Network Ser/Deser") {
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
    pingDeserO shouldBe a[NetMessage[Ping.type]];
    val pingDeser = pingDeserO.asInstanceOf[NetMessage[Ping.type]];
    pingDeser should equal (ping);

    buf.clear();

    Serializers.toBinary(pong, buf);
    val pongDeserO = Serializers.fromBinary(buf, noHint);
    pongDeserO shouldBe a[NetMessage[Pong.type]];
    val pongDeser = pongDeserO.asInstanceOf[NetMessage[Pong.type]];
    pongDeser should equal (pong);
  }

  test("Address Ser/Deser") {
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

  // Ignore because this test only runs once correctly and then requires a JVM restart...some UDT weirdness with SBT -.-
  test("Kompics Scala Network System") {

    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[Pinger](Init(latch, 100l, addr));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    println(s"Pinger Path is $addr");

    val pongerF = system.createNotify[Ponger](Init());
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
