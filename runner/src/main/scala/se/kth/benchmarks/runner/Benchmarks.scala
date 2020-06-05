package se.kth.benchmarks.runner

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import com.lkroll.common.macros.Macros
import se.kth.benchmarks.Statistics

import scala.concurrent.duration._

case class BenchmarkRun[Params](name: String, symbol: String, invoke: (Runner.Stub, Params) => Future[TestResult]);

object BenchMode extends Enumeration {
  type BenchMode = Value
  val NORMAL, TEST, CONVERGE = Value
}

trait Benchmark {
  def name: String;
  def symbol: String;
  def withStub(stub: Runner.Stub, testing: Boolean)(f: (Future[TestResult], ParameterDescription, Long) => Unit): Unit;
  def requiredRuns(testing: Boolean): Long;
}
object Benchmark {
  def apply[Params](b: BenchmarkRun[Params],
                    space: ParameterSpace[Params],
                    testSpace: ParameterSpace[Params],
                    convergeSpace: Option[ParameterSpace[Params]],
                    convergeFunction: Option[(Params, Long) => Long]): BenchmarkWithSpace[Params] =
    BenchmarkWithSpace(b, space, testSpace, convergeSpace, convergeFunction);
  def apply[Params](name: String,
                    symbol: String,
                    invoke: (Runner.Stub, Params) => Future[TestResult],
                    space: ParameterSpace[Params],
                    testSpace: ParameterSpace[Params],
                    convergeSpace: Option[ParameterSpace[Params]],
                    convergeFunction: Option[(Params, Long) => Long]): BenchmarkWithSpace[Params] =
    BenchmarkWithSpace(BenchmarkRun(name, symbol, invoke), space, testSpace, convergeSpace, convergeFunction);
}
case class BenchmarkWithSpace[Params](b: BenchmarkRun[Params],
                                      space: ParameterSpace[Params],
                                      testSpace: ParameterSpace[Params],
                                      convergeSpace: Option[ParameterSpace[Params]],
                                      convergeFunction: Option[(Params, Long) => Long])  // params, prev_result, current_result
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
      ParameterSpacePB.mapped(10L.k to 100L.k by 10L.k).msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n)),
    convergeSpace = None,
    convergeFunction = None
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
      ParameterSpacePB.mapped(100L to 1L.k by 100L).msg[PingPongRequest](n => PingPongRequest(numberOfMessages = n)),
    convergeSpace = None,
    convergeFunction = None
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
      },
      convergeSpace = None,
      convergeFunction = None
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
      },
    convergeSpace = None,
    convergeFunction = None
  );

  private val windowDataSize = 0.008; // 8kB in MB
  private val windowLengthUtil = utils.Conversions.SizeToTime(windowDataSize, 1.millisecond); // 8kB/s in MB
  private val windowLength = windowLengthUtil.timeForMB(windowDataSize); // 1s window
  val streamingWindows = Benchmark(
    name = "Streaming Windows",
    symbol = "STREAMINGWINDOWS",
    invoke = (stub, request: StreamingWindowsRequest) => {
      stub.streamingWindows(request)
    },
    space = ParameterSpacePB
      .cross(List(1, 2, 4, 8, 16), List(100, 1000), List(0.01, 1.0, 100.0), List(10))
      .msg[StreamingWindowsRequest] {
        case (np, bs, ws, nw) => {
          val windowAmp = (ws / windowDataSize).round;
          StreamingWindowsRequest(numberOfPartitions = np,
                                  batchSize = bs,
                                  windowSize = windowLength,
                                  numberOfWindows = nw,
                                  windowSizeAmplification = windowAmp)
        }
      },
    testSpace = ParameterSpacePB
      .cross(List(1, 2), List(100), List(0.01, 0.1, 1.0, 10.0), List(10))
      .msg[StreamingWindowsRequest] {
        case (np, bs, ws, nw) => {
          val windowAmp = (ws / windowDataSize).round;
          StreamingWindowsRequest(numberOfPartitions = np,
                                  batchSize = bs,
                                  windowSize = windowLength,
                                  numberOfWindows = nw,
                                  windowSizeAmplification = windowAmp)
        }
      },
    convergeSpace = None,
    convergeFunction = None
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
      .cross(List((0.5f, 0.5f)), List(3), List(1000))
      .msg[AtomicRegisterRequest] {
        case ((rwl, wwl), p, k) =>
          AtomicRegisterRequest(readWorkload = rwl, writeWorkload = wwl, partitionSize = p, numberOfKeys = k)
      },
    convergeSpace = None,
    convergeFunction = None
  );

  val fibonacci = Benchmark(
    name = "Fibonacci",
    symbol = "FIBONACCI",
    invoke = (stub, request: FibonacciRequest) => {
      stub.fibonacci(request)
    },
    space = ParameterSpacePB
      .mapped(26 to 32 by 1)
      .msg[FibonacciRequest](n => FibonacciRequest(fibNumber = n)),
    testSpace = ParameterSpacePB
      .mapped(22 to 28 by 1)
      .msg[FibonacciRequest](n => FibonacciRequest(fibNumber = n)),
    convergeSpace = None,
    convergeFunction = None
  );

  val chameneos = Benchmark(
    name = "Chameneos",
    symbol = "CHAMENEOS",
    invoke = (stub, request: ChameneosRequest) => {
      stub.chameneos(request)
    },
    space = ParameterSpacePB
      .cross(List(2, 3, 4, 5, 6, 8, 12, 16, 20, 24, 28, 32, 36, 40, 48, 56, 64, 128), List(2.mio))
      .msg[ChameneosRequest] {
        case (nc, nm) => ChameneosRequest(numberOfChameneos = nc, numberOfMeetings = nm)
      },
    testSpace = ParameterSpacePB
      .cross(List(2, 3, 4, 5, 6, 7, 8, 16), List(100.k))
      .msg[ChameneosRequest] {
        case (nc, nm) => ChameneosRequest(numberOfChameneos = nc, numberOfMeetings = nm)
      },
    convergeSpace = None,
    convergeFunction = None
  );

  val allPairsShortestPath = Benchmark(
    name = "All-Pairs Shortest Path",
    symbol = "APSP",
    invoke = (stub, request: APSPRequest) => {
      stub.allPairsShortestPath(request)
    },
    space = ParameterSpacePB
      .cross(List(128, 256, 512, 1024), List(16, 32, 64))
      .msg[APSPRequest] {
        case (nn, bs) => {
          assert(nn % bs == 0, "BlockSize must evenly divide nodes!");
          APSPRequest(numberOfNodes = nn, blockSize = bs)
        }
      },
    testSpace = ParameterSpacePB
      .cross(List(128, 192, 256), List(16, 32, 64))
      .msg[APSPRequest] {
        case (nn, bs) => {
          assert(nn % bs == 0, "BlockSize must evenly divide nodes!");
          APSPRequest(numberOfNodes = nn, blockSize = bs)
        }
      },
    convergeSpace = None,
    convergeFunction = None
  );

  /*** split into different parameter spaces as some parameters are dependent on each other ***/
  private val atomicBroadcastTestNodes = List(3, 5);
  private val atomicBroadcastTestProposals = List(8L.k, 16L.k, 32L.k);
  private val atomicBroadcastTestBatchSizes = List(1, 4L.k, 8L.k, 16L.k, 32L.k);

  private val atomicBroadcastNodes = List(3, 5, 7, 9);
  private val atomicBroadcastProposals = List(100L.k, 200L.k, 400L.k, 800L.k);
  private val atomicBroadcastBatchSizes = List(1, 10L.k, 50L.k, 100L.k);

  private val atomicBroadcastReconfigurations = List("off", "single");

  private val paxosNormalTestSpace = ParameterSpacePB // paxos test without reconfig
    .cross(
      List("paxos", "raft"),
      List(3),
      List(1L.k),
      List(1L.k),
      List("off"),
      List("none"),
      List(false)
    );

  private val paxosReconfigTestSpace = ParameterSpacePB // paxos test with reconfig
    .cross(
      List("paxos"),
      atomicBroadcastTestNodes,
      atomicBroadcastTestProposals,
      atomicBroadcastTestBatchSizes,
      List("single", "majority"),
      List("pull", "eager"),
      List(false)
    );

  private val paxosTestSpace = paxosNormalTestSpace.append(paxosReconfigTestSpace);

  private val raftTestSpace = ParameterSpacePB
    .cross(
      List("raft"),
      List(3),
      List(4L.k),
      List(2L.k),
      List("off"),
      List("none"),
      List(false)
    );

  private val paxosNormalSpace = ParameterSpacePB
    .cross(
      List("paxos"),
      atomicBroadcastNodes,
      atomicBroadcastProposals,
      atomicBroadcastBatchSizes,
      List("off"),
      List("none"),
      List(true, false)
    );

  private val paxosReconfigSpace = ParameterSpacePB
    .cross(
      List("paxos"),
      atomicBroadcastNodes,
      atomicBroadcastProposals,
      atomicBroadcastBatchSizes,
      List("single", "majority"),
      List("pull", "eager"),
      List(true, false)
    );

  private val paxosSpace = paxosNormalSpace.append(paxosReconfigSpace);

  private val raftSpace = ParameterSpacePB
    .cross(
      List("raft"),
      atomicBroadcastNodes,
      atomicBroadcastProposals,
      atomicBroadcastBatchSizes,
      atomicBroadcastReconfigurations,
      List("none"),
      List(false)
    );

  val atomicBroadcast = Benchmark(
    name = "Atomic Broadcast",
    symbol = "ATOMICBROADCAST",
    invoke = (stub, request: AtomicBroadcastRequest) => {
      stub.atomicBroadcast(request)
    },
    space = paxosSpace.append(raftSpace)
      .msg[AtomicBroadcastRequest] {
        case (a, nn, np, pp, r, tp, fd) =>
          AtomicBroadcastRequest(
            algorithm = a,
            numberOfNodes = nn,
            numberOfProposals = np,
            batchSize = pp,
            reconfiguration = r,
            transferPolicy = tp,
            forwardDiscarded = fd
          )
      },
    testSpace = paxosNormalTestSpace
      .msg[AtomicBroadcastRequest] {
        case (a, nn, np, pp, r, tp, fd) =>
          AtomicBroadcastRequest(
            algorithm = a,
            numberOfNodes = nn,
            numberOfProposals = np,
            batchSize = pp,
            reconfiguration = r,
            transferPolicy = tp,
            forwardDiscarded = fd
          )
      },
    convergeSpace = Some(paxosNormalTestSpace
      .msg[AtomicBroadcastRequest] {
        case (a, nn, np, pp, r, tp, fd) =>
          AtomicBroadcastRequest(
            algorithm = a,
            numberOfNodes = nn,
            numberOfProposals = np,
            batchSize = pp,
            reconfiguration = r,
            transferPolicy = tp,
            forwardDiscarded = fd
          )
      }),
    convergeFunction = Some((a: AtomicBroadcastRequest, l: Long) => {
      a.numberOfProposals/l
    })
  );

  val benchmarks: List[Benchmark] = Macros.memberList[Benchmark];
  lazy val benchmarkLookup: Map[String, Benchmark] = benchmarks.map(b => (b.symbol -> b)).toMap;
}
