package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import java.util.concurrent.Executors
import java.util.concurrent.ConcurrentLinkedQueue
import com.typesafe.scalalogging.StrictLogging

case class ClientEntry(address: String, port: Int, stub: BenchmarkClientGrpc.BenchmarkClient)

case class BenchRequest(f: () => Future[TestResult])

class BenchmarkMaster(val runnerPort: Int, val masterPort: Int, val waitFor: Int, val benchmarks: BenchmarkFactory)
    extends StrictLogging { self =>
  import BenchmarkRunner.{MAX_RUNS, MIN_RUNS, RSE_TARGET, failureToTestResult, measure, resultToTestResult, rse};

  import BenchmarkMaster.{IterationData, State};

  implicit val benchmarkPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());
  val serverPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());

  private var clients = List.empty[ClientEntry];
  //private var resultPromiseO: Option[Promise[TestResult]] = None;

  // Atomic
  private val state: State = State.init();

  private val benchQueue = new ConcurrentLinkedQueue[BenchRequest]();

  private object MasterService extends BenchmarkMasterGrpc.BenchmarkMaster {

    override def checkIn(request: ClientInfo): Future[CheckinResponse] = {
      if (state() == State.INIT) {
        logger.info(s"Got Check-In from ${request.address}:${request.port}");
        BenchmarkMaster.synchronized {
          clients ::= clientInfoToEntry(request);
          if (clients.size == waitFor) {
            logger.info(s"Got all ${clients.size} Check-Ins: Ready!");
            goReady();
          } else {
            logger.debug(s"Got ${clients.size}/${waitFor} Check-Ins.");
          }
        }
      } else {
        logger.warn(s"Ignoring late Check-In: $request");
      }
      Future.successful(CheckinResponse())
    }

  }

  private def goReady(): Unit = {
    state cas (State.INIT -> State.READY);
    def emptyQ(): Unit = {
      val br = benchQueue.poll();
      if (br != null) {
        val f = br.f();
        f.onComplete(_ => emptyQ())
      }
    };
    emptyQ();
  }

  private object RunnerService extends BenchmarkRunnerGrpc.BenchmarkRunner {
    override def ready(request: ReadyRequest): Future[ReadyResponse] = {
      if (state() == State.READY) {
        Future.successful(ReadyResponse(true))
      } else {
        Future.successful(ReadyResponse(false))
      }
    }

    override def pingPong(request: PingPongRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.pingPong;
      runBenchmark(b, request)
    };
    override def netPingPong(request: PingPongRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.netPingPong;
      runBenchmark(b, request)
    };
    override def netThroughputPingPong(request: ThroughputPingPongRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.netThroughputPingPong;
      runBenchmark(b, request)
    };
    override def throughputPingPong(request: ThroughputPingPongRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.throughputPingPong;
      runBenchmark(b, request)
    };
    override def atomicRegister(request: AtomicRegisterRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.atomicRegister;
      runBenchmark(b, request)
    };

    override def streamingWindows(request: StreamingWindowsRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.streamingWindows;
      runBenchmark(b, request)
    }

    override def allPairsShortestPath(request: APSPRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.allPairsShortestPath;
      runBenchmark(b, request)
    }
    override def chameneos(request: ChameneosRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.chameneos;
      runBenchmark(b, request)
    }
    override def fibonacci(request: FibonacciRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.fibonacci;
      runBenchmark(b, request)
    }
    override def atomicBroadcast(request: AtomicBroadcastRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.atomicBroadcast;
      runBenchmark(b, request)
    }
    override def sizedThroughput(request: SizedThroughputRequest): Future[TestResult] = queueIfNotReady {
      val b = benchmarks.sizedThroughput;
      runBenchmark(b, request)
    };
    override def shutdown(request: ShutdownRequest): Future[ShutdownAck] = {
      logger.info(s"Got shutdown request with force=${request.force}");

      logger.info(s"Forwarding shutdown request to ${clients.size} children.");
      val shutdownF = Future.sequence(clients.map { client =>
        client.stub.shutdown(request);
      });

      shutdownF.onComplete {
        case Success(_) => {
          logger.info(s"Shutting down.");
          state := State.STOPPED;
          stop();
        }
        case Failure(e) => {
          logger.warn("Some clients may have failed to shut down.", e);
          logger.info(s"Shutting down.");
          state := State.STOPPED;
          stop();
        }
      }
      if (request.force) {
        Util.forceShutdown();
      }
      shutdownF.map(_ => ShutdownAck())
    }

    private def queueIfNotReady(f: => Future[TestResult]): Future[TestResult] = {
      val handledF = () =>
        try {
          f
        } catch {
          case _: scala.NotImplementedError => Future.successful(NotImplemented())
          case e: Throwable                 => Future.failed(e)
        };
      if (state() == State.READY) {
        handledF()
      } else {
        val p = Promise[TestResult]();
        val func = () => {
          p.completeWith(handledF());
          p.future
        };
        benchQueue.offer(BenchRequest(func));
        p.future
      }
    }
  }

  private[this] var masterServer: Server = null;
  private[this] var runnerServer: Server = null;

  private[benchmarks] def start(): Unit = {
    import util.retry.blocking.{Failure, Retry, RetryStrategy, Success}

    implicit val retryStrategy = RetryStrategy.fixedBackOff(retryDuration = 500.milliseconds, maxAttempts = 10);

    masterServer = Retry {
      ServerBuilder
        .forPort(masterPort)
        .addService(BenchmarkMasterGrpc.bindService(MasterService, serverPool))
        .build()
        .start();
    } match {
      case Success(server) => {
        logger.info(s"Master Server started, listening on $masterPort");
        server
      }
      case Failure(ex) => {
        logger.error(s"Could not start BenchmarkMaster: $ex");
        throw ex;
      }
    };

    runnerServer = Retry {
      ServerBuilder
        .forPort(runnerPort)
        .addService(BenchmarkRunnerGrpc.bindService(RunnerService, serverPool))
        .build()
        .start();
    } match {
      case Success(server) => {
        logger.info(s"Runner Server started, listening on $runnerPort");
        server
      }
      case Failure(ex) => {
        logger.error(s"Could not start RunnerService: $ex");
        throw ex;
      }
    }

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private[benchmarks] def stop(): Unit = {
    if (masterServer != null) {
      masterServer.shutdown()
    }
    if (runnerServer != null) {
      runnerServer.shutdown()
    }
  }

  private[benchmarks] def blockUntilShutdown(): Unit = {
    if (masterServer != null) {
      masterServer.awaitTermination()
    }
    if (runnerServer != null) {
      runnerServer.awaitTermination()
    }
  }

  private def runBenchmark(b: Benchmark, msg: scalapb.GeneratedMessage): Future[TestResult] = {
    b.msgToConf(msg) match {
      case Success(c) => {
        state cas (State.READY -> State.RUN);
        logger.info(s"Starting local test ${b.getClass.getCanonicalName}");
        val f = Future {
          val r = BenchmarkRunner.run(b)(c);
          resultToTestResult(r)
        };
        f.onComplete(_ => {
          logger.info(s"Completed local test ${b.getClass.getCanonicalName}");
          state := State.READY;
        }); // no point to check state here, as the exception would be swallowed anyway
        f
      }
      case Failure(e) => Future.failed(e)
    }
  }

  private def runBenchmark(b: DistributedBenchmark, msg: scalapb.GeneratedMessage): Future[TestResult] = {
    b.msgToMasterConf(msg) match {
      case Success(masterConf) => {
        state cas (State.READY -> State.SETUP);
        val rp = Promise.apply[TestResult];
        val meta = DeploymentMetaData(clients.size);

        logger.info(s"Starting distributed test ${b.getClass.getCanonicalName}");

        val exF = Future {
          val master = b.newMaster();
          master.setup(masterConf, meta) match {
            case Success(clientConf) => {
              val clientConfS = b.clientConfToString(clientConf);
              val clientSetup = SetupConfig(b.getClass.getCanonicalName, clientConfS);
              val clientDataRLF = Future.sequence(clients.map(_.stub.setup(clientSetup)));
              val clientDataLF = clientDataRLF.flatMap(l => {
                logger.debug(s"Got ${l.length} setup responses.");
                val (successes, failures) = l.partition(sr => sr.success);
                if (failures.isEmpty) {
                  val deserRes = successes.map { sr =>
                    val cd = b.strToClientData(sr.data);
                    logger.trace(s"Setup response ${sr.data} deserialised to $cd");
                    cd
                  };
                  val (deserSuccesses, deserFailures) = deserRes.partition(_.isSuccess);
                  if (deserFailures.isEmpty) {
                    val deserData = deserSuccesses.map(_.get);
                    Future.successful(deserData)
                  } else {
                    val msg =
                      s"Client Data Deserialisation Errors: ${deserFailures.map(_.asInstanceOf[Failure[b.ClientData]]).map(_.exception.getMessage()).mkString("[", ";", "]")}";
                    Future.failed(new BenchmarkException(msg))
                  }
                } else {
                  val msg = s"Client Setup Errors: ${failures.map(_.data).mkString("[", ";", "]")}";
                  Future.failed(new BenchmarkException(msg))
                }
              });
              def iteration(clientDataL: List[b.ClientData],
                            nRuns: Int,
                            results: List[Double]): Future[IterationData] = {

                logger.debug(s"Preparing iteration $nRuns");
                Try(master.prepareIteration(clientDataL)) match {
                  case Success(_) => {
                    logger.debug(s"Starting iteration $nRuns");
                    Try(BenchmarkRunner.measure(master.runIteration)) match {
                      case Success(r) => {
                        logger.debug(s"Finished iteration $nRuns");
                        val incRuns = nRuns + 1;
                        val newResults = r :: results;
                        val done = !((incRuns < MIN_RUNS) || (incRuns < MAX_RUNS) && (rse(newResults) > RSE_TARGET));
                        if (done) {
                          state cas (State.RUN -> State.CLEANUP);
                        }
                        Try(master.cleanupIteration(done, r)) match {
                          case Success(_) => {
                            val f = Future.sequence(clients.map(_.stub.cleanup(CleanupInfo(done))));
                            val iterData = IterationData(incRuns, newResults, done);
                            f.map(_ => {
                              if (done) {
                                state cas (State.CLEANUP -> State.FINISHED);
                              }
                              iterData
                            })
                          }
                          case Failure(e) => {
                            logger.error(s"Failed during cleanupIteration $nRuns", e);
                            if (!done) { // try again in final mode
                              try { // this is likely to fail, but we should still try
                                master.cleanupIteration(true, 0);
                              } catch {
                                case e: Throwable => logger.error(s"Error during forced master cleanup!", e)
                              }
                            }
                            val f = Future.sequence(clients.map(_.stub.cleanup(CleanupInfo(true))));
                            f.transformWith(_ => Future.failed(e)) // ignore cleanup errors and return original failure
                          }
                        }
                      }
                      case Failure(e) => {
                        logger.error(s"Failed during runIteration $nRuns", e);
                        try { // this is likely to fail, but we should still try
                          master.cleanupIteration(true, 0);
                        } catch {
                          case e: Throwable => logger.error(s"Error during forced master cleanup!", e)
                        }
                        val f = Future.sequence(clients.map(_.stub.cleanup(CleanupInfo(true))));
                        f.transformWith(_ => Future.failed(e)) // ignore cleanup errors and return original failure
                      }
                    }
                  }
                  case Failure(e) => {
                    logger.error(s"Failed during prepareIteration $nRuns", e);
                    try { // this is likely to fail, but we should still try
                      master.cleanupIteration(true, 0);
                    } catch {
                      case e: Throwable => logger.error(s"Error during forced master cleanup!", e)
                    }
                    val f = Future.sequence(clients.map(_.stub.cleanup(CleanupInfo(true))));
                    f.transformWith(_ => Future.failed(e)) // ignore cleanup errors and return original failure
                  }
                }
              };
              clientDataLF.map { clientDataL =>
                state cas (State.SETUP -> State.RUN);
                logger.debug(s"Going to RUN state and starting first iteration.");
                // run until RSE target is met
                def loop(id: IterationData): Unit = {
                  if (id.done) {
                    val tr = resultToTestResult(Success(id.results));
                    logger.debug(s"Finished run.");
                    rp.success(tr)
                  } else {
                    val f = iteration(clientDataL, id.nRuns, id.results);
                    f.onComplete {
                      case Success(id) => loop(id)
                      case Failure(ex) => rp.failure(ex)
                    };
                  }
                };
                val f = iteration(clientDataL, 0, List.empty[Double]);
                f.onComplete {
                  case Success(id) => loop(id)
                  case Failure(ex) => rp.failure(ex)
                };
              };
            }
            case Failure(e) => {
              Future.failed(e)
            }
          }

        };
        exF.flatten.failed.foreach { e =>
          logger.error(s"runBenchmark failed!", e);
          rp.failure(e);
        }

        val f = rp.future.transform {
          case s @ Success(_) => s
          case f @ Failure(_) => Success(failureToTestResult(f))
        };
        f.onComplete(_ => {
          logger.info(s"Completed distributed test ${b.getClass.getCanonicalName}");
          state := State.READY; // reset state
        }); // no point to check state here, as the exception would be swallowed anyway
        f
      }
      case f @ Failure(_) => {
        //logger.warn("Master setup failed", e);
        Future.successful(failureToTestResult(f, Some("Deserialise Master Conf")))
      }
    }
  }

  private def clientInfoToEntry(ci: ClientInfo): ClientEntry = {
    val channel = ManagedChannelBuilder.forAddress(ci.address, ci.port).usePlaintext().build;
    val stub = BenchmarkClientGrpc.stub(channel);
    ClientEntry(ci.address, ci.port, stub)
  }

  // private def tryToFuture[T](t: Try[T]): Future[T] = t match {
  //   case Success(cd) => Future.successful(cd)
  //   case Failure(ex) => Future.failed(ex)
  // };
}

object BenchmarkMaster {

  def run(waitFor: Int, masterPort: Int, runnerPort: Int, benchF: BenchmarkFactory): Unit = {
    val inst = new BenchmarkMaster(runnerPort, masterPort, waitFor, benchF);
    inst.start();
    inst.blockUntilShutdown();
  }

  class State(initial: Int) {
    private val _inner: java.util.concurrent.atomic.AtomicInteger =
      new java.util.concurrent.atomic.AtomicInteger(initial);

    def :=(v: Int): Unit = _inner.set(v);
    def cas(oldValue: Int, newValue: Int): Unit = if (!_inner.compareAndSet(oldValue, newValue)) {
      throw new RuntimeException(s"Invalid State Transition from $oldValue -> $newValue")
    };
    def cas(t: (Int, Int)): Unit = cas(t._1, t._2);
    def apply(): Int = _inner.get;
  }
  object State {
    val INIT = 0;
    val READY = 1;
    val SETUP = 2;
    val RUN = 3;
    val CLEANUP = 4;
    val FINISHED = 5;
    val STOPPED = 6;

    def apply(v: Int): State = new State(v);
    def init(): State = apply(INIT);
  }

  case class IterationData(nRuns: Int, results: List[Double], done: Boolean)
}
