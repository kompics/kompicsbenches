package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.util.{ Try, Success, Failure }
import io.grpc.{ Server, ServerBuilder, ManagedChannelBuilder }
import java.util.concurrent.Executors
import com.typesafe.scalalogging.StrictLogging

class BenchmarkClient(
  val address:       String,
  val masterAddress: String,
  val masterPort:    Int) extends StrictLogging { self =>

  import BenchmarkClient.{ State, StateType, ActiveBench, MAX_ATTEMPTS };

  implicit val benchmarkPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());
  val serverPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());

  lazy val classLoader = this.getClass.getClassLoader;

  private val state: State = State.init();

  private object ClientService extends BenchmarkClientGrpc.BenchmarkClient {
    override def setup(request: SetupConfig): Future[SetupResponse] = {
      if (state() == StateType.CheckingIn) {
        state := StateType.Ready; // Clearly Check-In succeeded, even if the RPC was faulty
      }
      val benchClassName = request.label;
      logger.debug(s"Trying to set up $benchClassName.");
      val res = Try {
        val benchC = classLoader.loadClass(benchClassName);
        val bench = benchC.getField("MODULE$").get(benchC).asInstanceOf[DistributedBenchmark];
        val activeBench = new ActiveBench(bench);
        state cas (StateType.Ready -> StateType.Running(activeBench));
        val r = activeBench.setup(request);
        activeBench.prepare();
        logger.info(s"$benchClassName is set up.");
        r
      } flatten;
      val resp = res match {
        case Success(s) => SetupResponse(true, s);
        case Failure(ex) => {
          logger.error(s"Setup for test $benchClassName was not successful.", ex);
          SetupResponse(false, ex.getMessage);
        }
      }
      Future.successful(resp)
    }

    override def cleanup(request: CleanupInfo): Future[CleanupResponse] = {
      state() match {
        case StateType.Running(activeBench) => {
          logger.debug("Cleaning active bench.");
          if (request.`final`) {
            activeBench.cleanup(true);
            state := StateType.Ready;
            logger.info(s"${activeBench.name} is cleaned.");
          } else {
            activeBench.cleanup(false);
            activeBench.prepare();
          }
          Future.successful(CleanupResponse())
        }
        case s => throw new RuntimeException(s"Invalid State (found $s expected Running)")
      }
    }

    override def shutdown(request: ShutdownRequest): Future[ShutdownAck] = {
      logger.info(s"Got shutdown request with force=${request.force}");
      if (request.force) {
        Util.forceShutdown();
      }
      state := StateType.Stopped;
      Util.shutdownLater(stop);
      Future.successful(ShutdownAck())
    }

  }

  private[this] var server: Server = null;
  private[this] var checkInAttempts: Int = 0;

  private[benchmarks] def start(): Unit = {
    checkInAttempts += 1;
    server = ServerBuilder.forPort(0).addService(
      BenchmarkClientGrpc.bindService(ClientService, serverPool))
      .build
      .start;

    val port = server.getPort;
    logger.info(s"Client Server started, listening on $port");
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
    checkin();
  }

  private[benchmarks] def checkin(): Unit = {
    val port = server.getPort;
    val master = {
      val channel = ManagedChannelBuilder.forAddress(masterAddress, masterPort).usePlaintext().build;
      val stub = BenchmarkMasterGrpc.stub(channel);
      stub
    };
    val f = master.checkIn(ClientInfo(address, port));
    f.onComplete {
      case Success(_) => {
        logger.info(s"Check-In successful");
        state cas (StateType.CheckingIn -> StateType.Ready)
      }
      case Failure(ex) => {
        logger.error("Check-In failed!", ex);
        if (checkInAttempts > MAX_ATTEMPTS) {
          logger.warn("Giving up on Master and shutting down.");
          System.exit(1);
        } else {
          logger.info("Retrying connection establishment.");
          Thread.sleep(500);
          this.checkin();
        }
      }
    }
  }

  private[benchmarks] def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private[benchmarks] def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

}

object BenchmarkClient {

  val MAX_ATTEMPTS: Int = 5;

  def run(address: String, masterAddress: String, masterPort: Int): Unit = {
    val inst = new BenchmarkClient(address, masterAddress, masterPort);
    inst.start();
    inst.blockUntilShutdown();
  }

  class ActiveBench(b: DistributedBenchmark) {
    private class ActiveInstance(val bi: b.Client) {

    }
    private val instance = new ActiveInstance(b.newClient());

    def setup(sc: SetupConfig): Try[String] = {
      for {
        clientConfig <- b.strToClientConf(sc.data)
      } yield {
        val clientData = instance.bi.setup(clientConfig);
        b.clientDataToString(clientData)
      }
    }

    def prepare(): Unit = {
      instance.bi.prepareIteration();
    }
    def cleanup(lastIteration: Boolean): Unit = {
      instance.bi.cleanupIteration(lastIteration);
    }
    def name: String = b.getClass.getCanonicalName;
  }

  sealed trait StateType;
  object StateType {
    case object CheckingIn extends StateType;
    case object Ready extends StateType;
    case class Running(ab: ActiveBench) extends StateType;
    case object Stopped extends StateType;
  }

  class State {
    private var _inner: StateType = StateType.CheckingIn;

    def :=(v: StateType): Unit = this.synchronized {
      _inner = v;
    };
    def cas(oldValue: StateType, newValue: StateType): Unit = this.synchronized {
      if (_inner == oldValue) {
        _inner = newValue
      } else {
        throw new RuntimeException(s"Invalid State Transition from $oldValue -> $newValue")
      }
    };
    def cas(t: (StateType, StateType)): Unit = cas(t._1, t._2);
    def apply(): StateType = this.synchronized { _inner };
  }
  object State {
    def init(): State = new State;
  }
}
