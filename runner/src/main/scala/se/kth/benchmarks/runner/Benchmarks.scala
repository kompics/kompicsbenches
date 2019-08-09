package se.kth.benchmarks.runner

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.Future
import scala.util.{ Try, Success, Failure }
import com.lkroll.common.macros.Macros

case class BenchmarkRun[Params](
  name:   String,
  symbol: String,
  invoke: (Runner.Stub, Params) => Future[TestResult]);

trait Benchmark {
  def name: String;
  def symbol: String;
  def withStub(stub: Runner.Stub, testing: Boolean)(f: (Future[TestResult], ParameterDescription, Long) => Unit): Unit;
  def requiredRuns(testing: Boolean): Long;
}
object Benchmark {
  def apply[Params](b: BenchmarkRun[Params], space: ParameterSpace[Params], testSpace: ParameterSpace[Params]): BenchmarkWithSpace[Params] = BenchmarkWithSpace(b, space, testSpace);
  def apply[Params](
    name:      String,
    symbol:    String,
    invoke:    (Runner.Stub, Params) => Future[TestResult],
    space:     ParameterSpace[Params],
    testSpace: ParameterSpace[Params]): BenchmarkWithSpace[Params] = BenchmarkWithSpace(BenchmarkRun(name, symbol, invoke), space, testSpace);
}
case class BenchmarkWithSpace[Params](b: BenchmarkRun[Params], space: ParameterSpace[Params], testSpace: ParameterSpace[Params]) extends Benchmark {
  override def name: String = b.name;
  override def symbol: String = b.symbol;
  def run = b.invoke;
  override def withStub(stub: Runner.Stub, testing: Boolean)(f: (Future[TestResult], ParameterDescription, Long) => Unit): Unit = {
    var index = 0l;
    val useSpace = if (testing) testSpace else space;
    useSpace.foreach{ p =>
      index += 1l;
      f(run(stub, p), useSpace.describe(p), index)
    }
  }
  override def requiredRuns(testing: Boolean): Long = if (testing) testSpace.size else space.size;
}

object Benchmarks extends ParameterDescriptionImplicits {

  implicit class ExtLong(i: Long) {
    def mio: Long = i * 1000000l;
    def k: Long = i * 1000l;
  }

  //implicit def seq2param[T: ParameterDescriptor](s: Seq[T]): ParameterSpace[T] = ParametersSparse1D(s);

  val pingPong = Benchmark(
    name = "Ping Pong",
    symbol = "PINGPONG",
    invoke = (stub, request: PingPongRequest) => {
      stub.pingPong(request)
    },
    space = ParameterSpacePB.mapped(1l.mio to 10l.mio by 1l.mio).msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n)),
    testSpace = ParameterSpacePB.mapped(10l.k to 100l.k by 10l.k).msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n)));

  val netPingPong = Benchmark(
    name = "Net Ping Pong",
    symbol = "NETPINGPONG",
    invoke = (stub, request: PingPongRequest) => {
      stub.netPingPong(request)
    },
    space = ParameterSpacePB.mapped(1l.k to 10l.k by 1l.k).msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n)),
    testSpace = ParameterSpacePB.mapped(100l to 1l.k by 100l).msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n)));

  val throughputPingPong = Benchmark(
    name = "Throughput Ping Pong",
    symbol = "TPPINGPONG",
    invoke = (stub, request: ThroughputPingPongRequest) => {
      stub.throughputPingPong(request)
    },
    space = ParameterSpacePB.cross(
      List(1l.mio, 10l.mio),
      List(10, 50, 500),
      List(1, 2, 4, 8, 16, 24, 32, 34, 36, 38, 40),
      List(true, false)).msg[ThroughputPingPongRequest] {
        case (n, p, par, s) =>
          ThroughputPingPongRequest(
            messagesPerPair = n,
            pipelineSize = p,
            parallelism = par,
            staticOnly = s)
      },
    testSpace = ParameterSpacePB.cross(
      10l.k to 100l.k by 30l.k,
      List(10, 500),
      List(1, 4, 8),
      List(true, false)).msg[ThroughputPingPongRequest] {
        case (n, p, par, s) =>
          ThroughputPingPongRequest(
            messagesPerPair = n,
            pipelineSize = p,
            parallelism = par,
            staticOnly = s)
      });

  val netThroughputPingPong = Benchmark(
    name = "Net Throughput Ping Pong",
    symbol = "NETTPPINGPONG",
    invoke = (stub, request: ThroughputPingPongRequest) => {
      stub.netThroughputPingPong(request)
    },
    space = ParameterSpacePB.cross(
      List(1l.k, 10l.k, 20l.k),
      List(10, 100, 1000, 10000),
      List(1, 2, 4, 8, 16, 24),
      List(true, false)).msg[ThroughputPingPongRequest] {
        case (n, p, par, s) =>
          ThroughputPingPongRequest(
            messagesPerPair = n,
            pipelineSize = p,
            parallelism = par,
            staticOnly = s)
      },
    testSpace = ParameterSpacePB.cross(
      100l to 1l.k by 300l,
      List(10, 100, 1000),
      List(1, 4, 8),
      List(true, false)).msg[ThroughputPingPongRequest] {
        case (n, p, par, s) =>
          ThroughputPingPongRequest(
            messagesPerPair = n,
            pipelineSize = p,
            parallelism = par,
            staticOnly = s)
      });

  val atomicRegister = Benchmark(
    name = "Atomic Register",
    symbol = "ATOMICREGISTER",
    invoke = (stub, request: AtomicRegisterRequest) => {
      stub.atomicRegister(request)
    },
    space = ParameterSpacePB.cross(
      List( (0.5f, 0.5f), (0.95f, 0.05f) ),
      List(3, 5, 7, 9),
      List(10l.k, 20l.k, 40l.k, 80l.k)).msg[AtomicRegisterRequest] {
      case ((read_workload, write_workload), p, k) =>
        AtomicRegisterRequest(
          readWorkload = read_workload,
          writeWorkload = write_workload,
          partitionSize = p,
          numberOfKeys = k)
    },
    testSpace = ParameterSpacePB.cross(
      List( (0.5f, 0.5f), (0.95f, 0.05f) ),
      List(3, 5),
      List(500l, 1000l, 2000l)).msg[AtomicRegisterRequest] {
      case ((rwl, wwl), p, k) =>
        AtomicRegisterRequest(
          readWorkload = rwl,
          writeWorkload = wwl,
          partitionSize = p,
          numberOfKeys = k)
    });
  val benchmarks: List[Benchmark] = Macros.memberList[Benchmark];
  lazy val benchmarkLookup: Map[String, Benchmark] = benchmarks.map(b => (b.symbol -> b)).toMap;
}
