package se.kth.benchmarks.kompicsjava.bench

import java.util

import org.scalatest._

import scala.util.{Failure, Success, Try}
import se.kth.benchmarks.kompicsscala.{KompicsSystemProvider, NetAddress => SNetAddress}
import se.kth.benchmarks.kompicsjava._
import se.kth.benchmarks.kompicsjava.net._
import java.util.concurrent.CountDownLatch

import NetPingPong._
import se.sics.kompics.sl._

import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.scalalogging.StrictLogging

class DistributedTest extends FunSuite with Matchers with StrictLogging {

  implicit val ec = scala.concurrent.ExecutionContext.global;

  test("Network Ser/Deser") {

    import se.kth.benchmarks.kompicsjava.bench.netpingpong._
    import se.kth.benchmarks.kompicsjava.bench.atomicregister._
    import se.sics.kompics.network.netty.serialization.Serializers;
    import io.netty.buffer.{ByteBuf, Unpooled};
    import java.util.{HashSet, Optional}

    BenchNetSerializer.register();
    NetPingPongSerializer.register();
    AtomicRegisterSerializer.register();

    val noHint: Optional[Object] = Optional.empty();

    val addr = NetAddress.from("127.0.0.1", 12345);
    val addr2 = NetAddress.from("127.0.0.1", 45678); // larger port number that doesn't fit into a short
    val ping = NetMessage.viaTCP(addr, addr, Ping.event);
    val pong = NetMessage.viaTCP(addr, addr, Pong.event);

    val buf = Unpooled.directBuffer();

    Serializers.toBinary(addr, buf);
    val addrDeserO = Serializers.fromBinary(buf, noHint);
    addrDeserO shouldBe a[NetAddress];
    val addrDeser = addrDeserO.asInstanceOf[NetAddress];
    addrDeser should equal(addr);

    buf.clear();

    Serializers.toBinary(addr2, buf);
    val addr2DeserO = Serializers.fromBinary(buf, noHint);
    addr2DeserO shouldBe a[NetAddress];
    val addr2Deser = addr2DeserO.asInstanceOf[NetAddress];
    addr2Deser should equal(addr2);

    buf.clear();

    Serializers.toBinary(ping, buf);
    val pingDeserO = Serializers.fromBinary(buf, noHint);
    pingDeserO shouldBe a[NetMessage];
    val pingDeser = pingDeserO.asInstanceOf[NetMessage];
    pingDeser should equal(ping);

    buf.clear();

    Serializers.toBinary(pong, buf);
    val pongDeserO = Serializers.fromBinary(buf, noHint);
    pongDeserO shouldBe a[NetMessage];
    val pongDeser = pongDeserO.asInstanceOf[NetMessage];
    pongDeser should equal(pong);

    buf.clear();
  }

  test("Throughput Network Ser/Deser") {

    import se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong._
    import se.sics.kompics.network.netty.serialization.Serializers;
    import io.netty.buffer.{ByteBuf, Unpooled};
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
    addrDeser should equal(addr);

    buf.clear();

    Serializers.toBinary(addr2, buf);
    val addr2DeserO = Serializers.fromBinary(buf, noHint);
    addr2DeserO shouldBe a[NetAddress];
    val addr2Deser = addr2DeserO.asInstanceOf[NetAddress];
    addr2Deser should equal(addr2);

    buf.clear();

    Serializers.toBinary(sping, buf);
    val spingDeserO = Serializers.fromBinary(buf, noHint);
    spingDeserO shouldBe a[NetMessage];
    val spingDeser = spingDeserO.asInstanceOf[NetMessage];
    spingDeser should equal(sping);

    buf.clear();

    Serializers.toBinary(spong, buf);
    val spongDeserO = Serializers.fromBinary(buf, noHint);
    spongDeserO shouldBe a[NetMessage];
    val spongDeser = spongDeserO.asInstanceOf[NetMessage];
    spongDeser should equal(spong);

    buf.clear();

    Serializers.toBinary(ping, buf);
    val pingDeserO = Serializers.fromBinary(buf, noHint);
    pingDeserO shouldBe a[NetMessage];
    val pingDeser = pingDeserO.asInstanceOf[NetMessage];
    pingDeser should equal(ping);

    buf.clear();

    Serializers.toBinary(pong, buf);
    val pongDeserO = Serializers.fromBinary(buf, noHint);
    pongDeserO shouldBe a[NetMessage];
    val pongDeser = pongDeserO.asInstanceOf[NetMessage];
    pongDeser should equal(pong);
  }

  test("Address Ser/Deser") {
    val addr = SNetAddress.from("127.0.0.1", 12345).get;
    logger.debug(s"Original Address: $addr");
    val ser = NetPingPong.clientDataToString(addr);
    logger.debug(s"Serialised Address: $ser");
    val deser = NetPingPong.strToClientData(ser).get;
    logger.debug(s"Deserialised Address: $deser");
    deser.getIp() should equal(addr.getIp());
    deser.getPort() should equal(addr.getPort());
    deser should equal(addr);
  }

  test("Kompics Java Network System") {
    import se.kth.benchmarks.kompicsjava.bench.netpingpong._

    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[Pinger](new Pinger.Init(latch, 100L, addr.asJava));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    logger.debug(s"Pinger Path is $addr");

    val pongerF = system.createNotify[Ponger](Init.none[Ponger]);
    val ponger = Await.result(pongerF, 5.seconds);
    val pongerConnF = system.connectNetwork(ponger);
    Await.result(pongerConnF, 5.seconds);
    logger.debug(s"Ponger Path is $addr");
    val pongerStartF = system.startNotify(ponger);
    Await.result(pongerStartF, 5.seconds);
    //pongerStartF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));

    val pingerStartF = system.startNotify(pinger);
    //startF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));
    Await.result(pingerStartF, 5.seconds);

    logger.info("Awaiting test result");
    latch.await();

    system.terminate();
  }

  test("Kompics Java Throughput Network System (GC)") {
    import se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong._

    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[Pinger](new Pinger.Init(1, latch, 100L, 10, addr.asJava));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    logger.debug(s"Pinger Path is $addr");

    val pongerF = system.createNotify[Ponger](new Ponger.Init(1));
    val ponger = Await.result(pongerF, 5.seconds);
    val pongerConnF = system.connectNetwork(ponger);
    Await.result(pongerConnF, 5.seconds);
    logger.debug(s"Ponger Path is $addr");
    val pongerStartF = system.startNotify(ponger);
    Await.result(pongerStartF, 5.seconds);
    //pongerStartF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));

    val pingerStartF = system.startNotify(pinger);
    //startF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));
    Await.result(pingerStartF, 5.seconds);

    logger.info("Awaiting test result");
    latch.await();

    system.terminate();
  }

  test("Kompics Java Throughput Network System (Static)") {
    import se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong._

    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[StaticPinger](new StaticPinger.Init(1, latch, 100L, 2, addr.asJava));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    logger.debug(s"Pinger Path is $addr");

    val pongerF = system.createNotify[StaticPonger](new StaticPonger.Init(1));
    val ponger = Await.result(pongerF, 5.seconds);
    val pongerConnF = system.connectNetwork(ponger);
    Await.result(pongerConnF, 5.seconds);
    logger.debug(s"Ponger Path is $addr");
    val pongerStartF = system.startNotify(ponger);
    Await.result(pongerStartF, 5.seconds);
    //pongerStartF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));

    val pingerStartF = system.startNotify(pinger);
    //startF.failed.foreach(e => Console.err.println(s"Could not start pinger: $e"));
    Await.result(pingerStartF, 5.seconds);

    logger.info("Awaiting test result");
    latch.await();

    system.terminate();
  }

  test("Kompics Java Streaming Windows") {
    import StreamingWindows._;
    import se.kth.benchmarks.kompicsjava.bench.streamingwindows._;
    import scala.concurrent.{Await, Future, Promise};
    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val _ = StreamingWindows.newMaster(); // just to ensure that serialisers are loaded...

    implicit val ec = scala.concurrent.ExecutionContext.global;
    val timeout = 30.seconds;

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val numberOfPartitions = 2;
    val latch = new CountDownLatch(2);
    val numberOfWindows = 2L;
    val windowSize = 100.millis;
    val javaWindowSize = scala.compat.java8.DurationConverters.toJava(windowSize);
    val batchSize = 10L;
    val amplification = 5L;

    val componentsLF = (for (pid <- (0 until numberOfPartitions)) yield {
      val source = for {
        source <- system.createNotify[StreamSource](new StreamSource.Init(pid));
        _ <- system.connectNetwork(source);
        _ <- system.startNotify(source)
      } yield source;
      val windower = for {
        windower <- system
          .createNotify[Windower](new Windower.Init(pid, javaWindowSize, batchSize, amplification, addr.asJava));
        _ <- system.connectNetwork(windower);
        _ <- system.startNotify(windower)
      } yield windower;
      val sink = for {
        sink <- system
          .createNotify[StreamSink](new StreamSink.Init(pid, latch, numberOfWindows, addr.asJava));
        _ <- system.connectNetwork(sink)
      } yield sink;
      for {
        so <- source;
        w <- windower;
        si <- sink
      } yield (pid, so, w, si)
    }).toList;
    val componentsFL = Future.sequence(componentsLF);
    val components = Await.result(componentsFL, timeout);

    val sinksStartF = components.map { case (_, _, _, sink) => system.startNotify(sink) };
    Await.result(Future.sequence(sinksStartF), timeout);

    logger.info("Awaiting test result");
    latch.await();

    system.terminate();
  }
}