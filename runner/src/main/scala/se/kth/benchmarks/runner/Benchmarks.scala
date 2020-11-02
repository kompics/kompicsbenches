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
      .cross(List(10L.mio), List(10, 500), List(1, 2, 4, 8, 16, 24, 32, 48, 64, 128), List(false))
      .msg[ThroughputPingPongRequest] {
        case (n, p, par, s) =>
          ThroughputPingPongRequest(messagesPerPair = n, pipelineSize = p, parallelism = par, staticOnly = s)
      },
    testSpace = ParameterSpacePB
      .cross(10L.k to 100L.k by 30L.k, List(10, 500), List(1, 32, 128), List(false))
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

  val sizedThroughput = Benchmark(
    name = "SizedThroughput",
    symbol = "SIZEDTP",
    invoke = (stub, request: SizedThroughputRequest) => {
      stub.sizedThroughput(request)
    },
    space = ParameterSpacePB
      .cross(List(10, 100, 1000), List(100), List(100), List(1, 2, 4, 8, 16))
      .msg[SizedThroughputRequest] {
        case (msize, bsize, bcount, pairs) =>
          SizedThroughputRequest(messageSize = msize, batchSize = bsize, numberOfBatches = bcount, numberOfPairs = pairs)
      },
    testSpace = ParameterSpacePB
      .cross(List(10, 100, 10000), List(10, 100), List(100), List(1, 8, 32, 128))
      .msg[SizedThroughputRequest] {
        case (msize, bsize, bcount, pairs) =>
          SizedThroughputRequest(messageSize = msize, batchSize = bsize, numberOfBatches = bcount, numberOfPairs = pairs)
      }
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
      .cross(List(1, 2, 4, 8, 16, 24, 32), List(100), List(0.01, 1.0, 10.0), List(10))
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
      }
  );

  val atomicRegister = Benchmark(
    name = "Atomic Register",
    symbol = "ATOMICREGISTER",
    invoke = (stub, request: AtomicRegisterRequest) => {
      stub.atomicRegister(request)
    },
    space = ParameterSpacePB
      .cross(List((0.5f, 0.5f), (0.95f, 0.05f)), List(3), List(10L.k, 20L.k, 40L.k, 80L.k))
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
      }
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
      .msg[FibonacciRequest](n => FibonacciRequest(fibNumber = n))
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
      }
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
      }
  );

  /*** split into different parameter spaces as some parameters are dependent on each other ***/
  private val atomicBroadcastTestNodes = List(3);
  private val atomicBroadcastTestProposals = List(1L.k);
  private val atomicBroadcastTestConcurrentProposals = List(200L);

  private val atomicBroadcastNodes = List(3, 5);
  private val atomicBroadcastProposals = List(20L.mio);
  private val atomicBroadcastConcurrentProposals = List(10L.k, 100L.k, 1L.mio);

  private val paxos = List("paxos");

  private val raft = List("raft");
  private val raft_reconfig = List("replace-follower", "replace-leader");

  private val paxos_reconfig = List("pull");

  private val paxosNormalTestSpace = ParameterSpacePB // paxos test without reconfig
    .cross(
      paxos,
      atomicBroadcastTestNodes,
      atomicBroadcastTestProposals,
      atomicBroadcastTestConcurrentProposals,
      List("off"),
      List("none"),
    );

  private val paxosReconfigTestSpace = ParameterSpacePB // paxos test with reconfig
    .cross(
      paxos,
      atomicBroadcastTestNodes,
      atomicBroadcastTestProposals,
      atomicBroadcastTestConcurrentProposals,
      List("single"),
      paxos_reconfig,
    );

  private val paxosTestSpace = paxosNormalTestSpace.append(paxosReconfigTestSpace);

  private val raftNormalTestSpace = ParameterSpacePB
    .cross(
      raft,
      atomicBroadcastTestNodes,
      atomicBroadcastTestProposals,
      atomicBroadcastTestConcurrentProposals,
      List("off"),
      List("none"),
    );

  private val raftReconfigTestSpace = ParameterSpacePB
    .cross(
      raft,
      atomicBroadcastTestNodes,
      atomicBroadcastTestProposals,
      atomicBroadcastTestConcurrentProposals,
      List("single"),
      raft_reconfig,
    );

  private val raftTestSpace = raftNormalTestSpace.append(raftReconfigTestSpace);

  private val paxosNormalSpace = ParameterSpacePB
    .cross(
      paxos,
      atomicBroadcastNodes,
      atomicBroadcastProposals,
      atomicBroadcastConcurrentProposals,
      List("off"),
      List("none"),
    );

  private val paxosReconfigSpace = ParameterSpacePB
    .cross(
      paxos,
      atomicBroadcastNodes,
      atomicBroadcastProposals,
      atomicBroadcastConcurrentProposals,
      List("single"),
      paxos_reconfig,
    );

  private val paxosSpace = paxosNormalSpace.append(paxosReconfigSpace);

  private val raftNormalSpace = ParameterSpacePB
    .cross(
      raft,
      atomicBroadcastNodes,
      atomicBroadcastProposals,
      atomicBroadcastConcurrentProposals,
      List("off"),
      List("none"),
    );

  private val raftReconfigSpace = ParameterSpacePB
    .cross(
      raft,
      atomicBroadcastNodes,
      atomicBroadcastProposals,
      atomicBroadcastConcurrentProposals,
      List("single"),
      raft_reconfig,
    );

  private val raftSpace = raftNormalSpace.append(raftReconfigSpace);

  private val latencySpace = ParameterSpacePB
    .cross(
      List("paxos", "raft"),
      List(3),
      List(1L.k),
      List(1L),
      List("off"),
      List("none"),
    );

  val atomicBroadcast = Benchmark(
    name = "Atomic Broadcast",
    symbol = "ATOMICBROADCAST",
    invoke = (stub, request: AtomicBroadcastRequest) => {
      stub.atomicBroadcast(request)
    },
    space = paxosSpace.append(raftSpace).append(latencySpace)
      .msg[AtomicBroadcastRequest] {
        case (a, nn, np, cp, r, rp) =>
          AtomicBroadcastRequest(
            algorithm = a,
            numberOfNodes = nn,
            numberOfProposals = np,
            concurrentProposals = cp,
            reconfiguration = r,
            reconfigPolicy = rp,
          )
      },
    testSpace = paxosReconfigTestSpace.append(raftReconfigTestSpace)
      .msg[AtomicBroadcastRequest] {
        case (a, nn, np, cp, r, rp) =>
          AtomicBroadcastRequest(
            algorithm = a,
            numberOfNodes = nn,
            numberOfProposals = np,
            concurrentProposals = cp,
            reconfiguration = r,
            reconfigPolicy = rp,
          )
      }
  );

  val benchmarks: List[Benchmark] = Macros.memberList[Benchmark];
  lazy val benchmarkLookup: Map[String, Benchmark] = benchmarks.map(b => (b.symbol -> b)).toMap;
}
