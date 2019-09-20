package se.kth.benchmarks.runner

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import com.lkroll.common.macros.Macros
import scala.concurrent.duration._

case class BenchmarkRun[Params](name: String, symbol: String, invoke: (Runner.Stub, Params) => Future[TestResult]);

trait Benchmark {
  def name: String;
  def symbol: String;
  def withStub(stub: Runner.Stub, testing: Boolean)(f: (Future[TestResult], ParameterDescription, Long) => Unit): Unit;
  def requiredRuns(testing: Boolean): Long;
}
object Benchmark {
  def apply[Params](b: BenchmarkRun[Params],
                    space: ParameterSpace[Params],
                    testSpace: ParameterSpace[Params]): BenchmarkWithSpace[Params] =
    BenchmarkWithSpace(b, space, testSpace);
  def apply[Params](name: String,
                    symbol: String,
                    invoke: (Runner.Stub, Params) => Future[TestResult],
                    space: ParameterSpace[Params],
                    testSpace: ParameterSpace[Params]): BenchmarkWithSpace[Params] =
    BenchmarkWithSpace(BenchmarkRun(name, symbol, invoke), space, testSpace);
}
case class BenchmarkWithSpace[Params](b: BenchmarkRun[Params],
                                      space: ParameterSpace[Params],
                                      testSpace: ParameterSpace[Params])
    extends Benchmark {
  override def name: String = b.name;
  override def symbol: String = b.symbol;
  def run = b.invoke;
  override def withStub(stub: Runner.Stub,
                        testing: Boolean)(f: (Future[TestResult], ParameterDescription, Long) => Unit): Unit = {
    var index = 0L;
    val useSpace = if (testing) testSpace else space;
    useSpace.foreach { p =>
      index += 1L;
      f(run(stub, p), useSpace.describe(p), index)
    }
  }
  override def requiredRuns(testing: Boolean): Long = if (testing) testSpace.size else space.size;
}

object Benchmarks extends ParameterDescriptionImplicits {

  implicit class ExtLong(i: Long) {
    def mio: Long = i * 1000000L;
    def k: Long = i * 1000L;
  }

  //implicit def seq2param[T: ParameterDescriptor](s: Seq[T]): ParameterSpace[T] = ParametersSparse1D(s);

  val pingPong = Benchmark(
    name = "Ping Pong",
    symbol = "PINGPONG",
    invoke = (stub, request: PingPongRequest) => {
      stub.pingPong(request)
    },
    space = ParameterSpacePB
      .mapped(1L.mio to 10L.mio by 1L.mio)
      .msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n)),
    testSpace =
      ParameterSpacePB.mapped(10L.k to 100L.k by 10L.k).msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n))
  );

  val netPingPong = Benchmark(
    name = "Net Ping Pong",
    symbol = "NETPINGPONG",
    invoke = (stub, request: PingPongRequest) => {
      stub.netPingPong(request)
    },
    space =
      ParameterSpacePB.mapped(1L.k to 10L.k by 1L.k).msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n)),
    testSpace =
      ParameterSpacePB.mapped(100L to 1L.k by 100L).msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n))
  );

  val throughputPingPong = Benchmark(
    name = "Throughput Ping Pong",
    symbol = "TPPINGPONG",
    invoke = (stub, request: ThroughputPingPongRequest) => {
      stub.throughputPingPong(request)
    },
    space = ParameterSpacePB
      .cross(List(1L.mio, 10L.mio), List(10, 50, 500), List(1, 2, 4, 8, 16, 24, 32, 34, 36, 38, 40), List(true, false))
      .msg[ThroughputPingPongRequest] {
        case (n, p, par, s) =>
          ThroughputPingPongRequest(messagesPerPair = n, pipelineSize = p, parallelism = par, staticOnly = s)
      },
    testSpace = ParameterSpacePB
      .cross(10L.k to 100L.k by 30L.k, List(10, 500), List(1, 4, 8), List(true, false))
      .msg[ThroughputPingPongRequest] {
        case (n, p, par, s) =>
          ThroughputPingPongRequest(messagesPerPair = n, pipelineSize = p, parallelism = par, staticOnly = s)
      }
  );

  val netThroughputPingPong = Benchmark(
    name = "Net Throughput Ping Pong",
    symbol = "NETTPPINGPONG",
    invoke = (stub, request: ThroughputPingPongRequest) => {
      stub.netThroughputPingPong(request)
    },
    space = ParameterSpacePB
      .cross(List(1L.k, 10L.k, 20L.k), List(10, 100, 1000), List(1, 2, 4, 8, 16, 24), List(true, false))
      .msg[ThroughputPingPongRequest] {
        case (n, p, par, s) =>
          ThroughputPingPongRequest(messagesPerPair = n, pipelineSize = p, parallelism = par, staticOnly = s)
      },
    testSpace = ParameterSpacePB
      .cross(100L to 1L.k by 300L, List(10, 100, 1000), List(1, 4, 8), List(true, false))
      .msg[ThroughputPingPongRequest] {
        case (n, p, par, s) =>
          ThroughputPingPongRequest(messagesPerPair = n, pipelineSize = p, parallelism = par, staticOnly = s)
      }
  );

  private val windowLengthUtil = utils.Conversions.SizeToTime(8.0 / 1000.0, 1.millisecond); // 8kB/s in MB
  val streamingWindows = Benchmark(
    name = "Streaming Windows",
    symbol = "STREAMINGWINDOWS",
    invoke = (stub, request: StreamingWindowsRequest) => {
      stub.streamingWindows(request)
    },
    space = ParameterSpacePB
      .cross(List(1, 2, 4, 8, 16), List(10, 100, 1000), List(0.01, 0.1, 1.0, 10.0), List(1, 10, 100))
      .msg[StreamingWindowsRequest] {
        case (np, bs, ws, nw) => {
          val windowLength = windowLengthUtil.timeForMB(ws);
          StreamingWindowsRequest(numberOfPartitions = np,
                                  batchSize = bs,
                                  windowSize = windowLength,
                                  numberOfWindows = nw)
        }
      },
    testSpace = ParameterSpacePB
      .cross(List(1, 2), List(10, 100), List(0.01, 0.1), List(1, 10))
      .msg[StreamingWindowsRequest] {
        case (np, bs, ws, nw) => {
          val windowLength = windowLengthUtil.timeForMB(ws);
          StreamingWindowsRequest(numberOfPartitions = np,
                                  batchSize = bs,
                                  windowSize = windowLength,
                                  numberOfWindows = nw)
        }
      }
  );

  val atomicRegister = Benchmark(
    name = "Atomic Register",
    symbol = "ATOMICREGISTER",
    invoke = (stub, request: AtomicRegisterRequest) => {
      stub.atomicRegister(request)
    },
    space = ParameterSpacePB
      .cross(List((0.5f, 0.5f), (0.95f, 0.05f)), List(3, 5, 7, 9), List(10L.k, 20L.k, 40L.k, 80L.k))
      .msg[AtomicRegisterRequest] {
        case ((read_workload, write_workload), p, k) =>
          AtomicRegisterRequest(readWorkload = read_workload,
                                writeWorkload = write_workload,
                                partitionSize = p,
                                numberOfKeys = k)
      },
    testSpace = ParameterSpacePB
      .cross(List((0.5f, 0.5f), (0.95f, 0.05f)), List(3, 5), List(500, 1000, 2000))
      .msg[AtomicRegisterRequest] {
        case ((rwl, wwl), p, k) =>
          AtomicRegisterRequest(readWorkload = rwl, writeWorkload = wwl, partitionSize = p, numberOfKeys = k)
      }
  );
  val benchmarks: List[Benchmark] = Macros.memberList[Benchmark];
  lazy val benchmarkLookup: Map[String, Benchmark] = benchmarks.map(b => (b.symbol -> b)).toMap;
}
