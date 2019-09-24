package se.kth.benchmarks.kompicsscala.bench

import org.scalatest._
import scala.util.{Failure, Success, Try}
import se.kth.benchmarks.kompicsscala._
import java.util.concurrent.CountDownLatch
import se.kth.benchmarks.kompicsscala.bench.AtomicRegister._
import se.sics.kompics.sl._
import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.scalalogging.StrictLogging

class DistributedTest extends FunSuite with Matchers with StrictLogging {

  implicit val ec = scala.concurrent.ExecutionContext.global;

  test("Network Ser/Deser") {
    import NetPingPong._
    import se.sics.kompics.network.netty.serialization.Serializers;
    import io.netty.buffer.{ByteBuf, Unpooled};
    import java.util.Optional;

    BenchNet.registerSerializers();
    NetPingPongSerializer.register();
    AtomicRegisterSerializer.register();

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
    pingDeserO shouldBe a[NetMessage[_]];
    val pingDeser = pingDeserO.asInstanceOf[NetMessage[Ping.type]];
    pingDeser should equal(ping);

    buf.clear();

    Serializers.toBinary(pong, buf);
    val pongDeserO = Serializers.fromBinary(buf, noHint);
    pongDeserO shouldBe a[NetMessage[_]];
    val pongDeser = pongDeserO.asInstanceOf[NetMessage[Pong.type]];
    pongDeser should equal(pong);
    /*
    buf.clear();
    // Atomic Register events
    val rid = 123
    val ts = 1
    val wr = 2
    val v = 3
    val rank = 4
    val init_id = -1
    val nodes: Set[NetAddress] = Set(addr, addr2)
    val init = NetMessage.viaTCP(addr, addr)(INIT(rank, init_id, nodes))
    val read = NetMessage.viaTCP(addr, addr)(READ(rid))
    val ack = NetMessage.viaTCP(addr, addr)(ACK(rid))
    val write = NetMessage.viaTCP(addr, addr)(WRITE(rid, ts, wr, v))
    val value = NetMessage.viaTCP(addr, addr)(VALUE(rid, ts, wr, v))
    val done = NetMessage.viaTCP(addr, addr)(DONE)

    Serializers.toBinary(done, buf)
    val doneDeserO = Serializers.fromBinary(buf, noHint);
    doneDeserO shouldBe a[NetMessage[_]];
    val doneDeser = doneDeserO.asInstanceOf[NetMessage[DONE.type]];
    doneDeser should equal (done);

    buf.clear()

    Serializers.toBinary(init, buf)
    val initDeserN = Serializers.fromBinary(buf, noHint)
    initDeserN shouldBe a[NetMessage[_]]
    val initDeser = initDeserN.asInstanceOf[NetMessage[_]].payload
    initDeser shouldBe a[INIT]
    initDeser.asInstanceOf[INIT].rank should be (rank)
    initDeser.asInstanceOf[INIT].init_id should be (init_id)
    initDeser.asInstanceOf[INIT].nodes should equal (nodes)

    buf.clear()

    Serializers.toBinary(read, buf)
    val readDeserN = Serializers.fromBinary(buf, noHint)
    readDeserN shouldBe a[NetMessage[_]]
    val readDeser = readDeserN.asInstanceOf[NetMessage[_]].payload
    readDeser shouldBe a[READ]
    readDeser.asInstanceOf[READ].rid should be (rid)

    buf.clear()

    Serializers.toBinary(ack, buf)
    val ackDeserN = Serializers.fromBinary(buf, noHint)
    ackDeserN shouldBe a[NetMessage[_]]
    val ackDeser = ackDeserN.asInstanceOf[NetMessage[_]].payload
    ackDeser shouldBe a[ACK]
    ackDeser.asInstanceOf[ACK].rid should be (rid)

    buf.clear()

    Serializers.toBinary(write, buf)
    val writeDeserN = Serializers.fromBinary(buf, noHint)
    writeDeserN shouldBe a[NetMessage[_]]
    val writeDeserO = writeDeserN.asInstanceOf[NetMessage[_]].payload
    writeDeserO shouldBe a[WRITE]
    val writeDeser = writeDeserO.asInstanceOf[WRITE]
    writeDeser.rid should be (rid)
    writeDeser.ts should be (ts)
    writeDeser.wr should be (wr)
    writeDeser.value should be (v)

    buf.clear()

    Serializers.toBinary(value, buf);
    val valueDeserN = Serializers.fromBinary(buf, noHint)
    valueDeserN shouldBe a[NetMessage[_]]
    val valueDeserO = valueDeserN.asInstanceOf[NetMessage[_]].payload
    valueDeserO shouldBe a[VALUE]
    val valueDeser = valueDeserO.asInstanceOf[VALUE]
    valueDeser.rid should be (rid)
    valueDeser.ts should be (ts)
    valueDeser.wr should be (wr)
    valueDeser.value should be (v)
   */
  }

  test("Throughput Network Ser/Deser") {
    import NetThroughputPingPong._
    import se.sics.kompics.network.netty.serialization.Serializers;
    import io.netty.buffer.{ByteBuf, Unpooled};
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
    spingDeserO shouldBe a[NetMessage[_]];
    val spingDeser = spingDeserO.asInstanceOf[NetMessage[StaticPing]];
    spingDeser should equal(sping);

    buf.clear();

    Serializers.toBinary(spong, buf);
    val spongDeserO = Serializers.fromBinary(buf, noHint);
    spongDeserO shouldBe a[NetMessage[_]];
    val spongDeser = spongDeserO.asInstanceOf[NetMessage[StaticPong]];
    spongDeser should equal(spong);

    buf.clear();

    Serializers.toBinary(ping, buf);
    val pingDeserO = Serializers.fromBinary(buf, noHint);
    pingDeserO shouldBe a[NetMessage[_]];
    val pingDeser = pingDeserO.asInstanceOf[NetMessage[Ping]];
    pingDeser should equal(ping);

    buf.clear();

    Serializers.toBinary(pong, buf);
    val pongDeserO = Serializers.fromBinary(buf, noHint);
    pongDeserO shouldBe a[NetMessage[_]];
    val pongDeser = pongDeserO.asInstanceOf[NetMessage[Pong]];
    pongDeser should equal(pong);
  }

  test("NetPingPong Ser/Deser") {
    val addr = NetAddress.from("127.0.0.1", 12345).get;
    logger.debug(s"Original Address: $addr");
    val ser = NetPingPong.clientDataToString(addr);
    logger.debug(s"Serialised Address: $ser");
    val deser = NetPingPong.strToClientData(ser).get;
    logger.debug(s"Deserialised Address: $deser");
    deser.getIp() should equal(addr.getIp());
    deser.getPort() should equal(addr.getPort());
    deser should equal(addr);
  }

  test("NetThroughputPingPong Ser/Deser") {
    val addr = NetAddress.from("127.0.0.1", 12345).get;
    logger.debug(s"Original Address: $addr");
    val ser = NetThroughputPingPong.clientDataToString(addr);
    logger.debug(s"Serialised Address: $ser");
    val deser = NetThroughputPingPong.strToClientData(ser).get;
    logger.debug(s"Deserialised Address: $deser");
    deser.getIp() should equal(addr.getIp());
    deser.getPort() should equal(addr.getPort());
    deser should equal(addr);
  }

  test("Kompics Scala Network System") {
    import NetPingPong._;
    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[Pinger](Init(latch, 100L, addr));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    logger.debug(s"Pinger Path is $addr");

    val pongerF = system.createNotify[Ponger](Init.none);
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

  test("Kompics Scala Throughput Network System (Static)") {
    import NetThroughputPingPong._;
    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[StaticPinger](Init(1, latch, 100L, 2L, addr));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    logger.debug(s"Pinger Path is $addr");

    val pongerF = system.createNotify[StaticPonger](Init(1));
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

  test("Kompics Scala Throughput Network System (GC)") {
    import NetThroughputPingPong._;
    KompicsSystemProvider.setPublicIf("127.0.0.1");

    val system = KompicsSystemProvider.newRemoteKompicsSystem(2);

    val addr = system.networkAddress.get;

    val latch = new CountDownLatch(1);
    val pingerIdF = system.createNotify[Pinger](Init(1, latch, 100L, 2L, addr));
    val pinger = Await.result(pingerIdF, 5.second);
    val pingerConnF = system.connectNetwork(pinger);
    Await.result(pingerConnF, 5.seconds);
    logger.debug(s"Pinger Path is $addr");

    val pongerF = system.createNotify[Ponger](Init(1));
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

  test("Kompics Scala Streaming Windows") {
    import StreamingWindows._;
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
    val batchSize = 10L;
    val amplification = 5L;

    val componentsLF = (for (pid <- (0 until numberOfPartitions)) yield {
      val source = for {
        source <- system.createNotify[StreamSource](Init[StreamSource](pid));
        _ <- system.connectNetwork(source);
        _ <- system.startNotify(source)
      } yield source;
      val windower = for {
        windower <- system
          .createNotify[Windower](WindowerInit(pid, windowSize, batchSize, amplification, addr));
        _ <- system.connectNetwork(windower);
        _ <- system.startNotify(windower)
      } yield windower;
      val sink = for {
        sink <- system
          .createNotify[StreamSink](Init[StreamSink](pid, latch, numberOfWindows, addr));
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
