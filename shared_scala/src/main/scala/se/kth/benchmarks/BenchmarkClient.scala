package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}

import java.util.concurrent.Executors
import com.typesafe.scalalogging.StrictLogging

import scala.language.postfixOps

class BenchmarkClient(val address: String, val masterAddress: String, val masterPort: Int) extends StrictLogging {
  self =>

  import BenchmarkClient.{ActiveBench, MAX_ATTEMPTS, State, StateType};

  implicit val benchmarkPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());
  val serverPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());

  lazy val classLoader = this.getClass.getClassLoader;

  private val state: State = State.init();

  private def classGetField(c: Class[_], fieldName: String): Option[java.lang.reflect.Field] = {
    for (f <- c.getFields()) {
      if (f.getName().equals(fieldName)) {
        return Some(f);
      }
    }
    return None;
  }

  private object ClientService extends BenchmarkClientGrpc.BenchmarkClient {
    override def setup(request: SetupConfig): Future[SetupResponse] = {
      if (state() == StateType.CheckingIn) {
        state := StateType.Ready; // Clearly Check-In succeeded, even if the RPC was faulty
      }
      val benchClassName = request.label;
      logger.debug(s"Trying to set up $benchClassName.");
      val res = Try {
        val benchC = classLoader.loadClass(benchClassName);
        val bench = classGetField(benchC, "MODULE$") match {
          case Some(f) => f.get(benchC).asInstanceOf[DistributedBenchmark] // is object
          case None    => benchC.newInstance().asInstanceOf[DistributedBenchmark] // is class
        }
        val activeBench = new ActiveBench(bench);
        state cas (StateType.Ready -> StateType.Running(activeBench));
        val r = activeBench.setup(request);
        if (r.isSuccess) {
          activeBench.prepare();
        }
        r
      } flatten;
      val resp = res match {
        case Success(s) => {
          logger.info(s"$benchClassName is set up.");
          SetupResponse(true, s);
        }
        case Failure(ex) => {
          logger.error(s"Setup for test $benchClassName was not successful.", ex);
          state := StateType.Ready; // reset state
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
            try {
              activeBench.cleanup(true);
              logger.info(s"${activeBench.name} is cleaned.");
            } catch {
              case e: Throwable => {
                logger.error(s"Error during final cleanup of ${activeBench.name}!", e);
                return Future.failed(e);
              }
            } finally {
              state := StateType.Ready;
            }
          } else {
            try {
              activeBench.cleanup(false);
              activeBench.prepare();
            } catch {
              case e: Throwable => {
                logger.error(s"Error during cleanup&prepare of ${activeBench.name}!", e);
                state := StateType.Ready;
                return Future.failed(e);
              }
            }
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
    import util.retry.blocking.{Failure, Retry, RetryStrategy, Success}

    implicit val retryStrategy = RetryStrategy.fixedBackOff(retryDuration = 500.milliseconds, maxAttempts = 10);

    server = Retry {
      ServerBuilder
        .forPort(0)
        .addService(BenchmarkClientGrpc.bindService(ClientService, serverPool))
        .build()
        .start();
    } match {
      case Success(server) => {
        val port = server.getPort;
        logger.info(s"Client Server started, listening on $port");
        server
      }
      case Failure(ex) => {
        logger.error(s"Could not start ClientService: $ex");
        throw ex;
      }
    };

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
    checkin();
  }

  private[benchmarks] def checkin(): Unit = {
    checkInAttempts += 1;
    val port = server.getPort;
    val channel = ManagedChannelBuilder.forAddress(masterAddress, masterPort).usePlaintext().build();
    val master = BenchmarkMasterGrpc.stub(channel);

    val f = master.checkIn(ClientInfo(address, port));
    f.onComplete {
      case Success(_) => {
        logger.info(s"Check-In successful");
        state cas (StateType.CheckingIn -> StateType.Ready);
        // always clean up
        channel.shutdownNow();
        channel.awaitTermination(500, java.util.concurrent.TimeUnit.MILLISECONDS);
      }
      case Failure(ex) => {
        logger.error("Check-In failed!", ex);
        channel.shutdownNow();
        channel.awaitTermination(500, java.util.concurrent.TimeUnit.MILLISECONDS);
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
    private class ActiveInstance(val bi: b.Client) {}
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
        throw new RuntimeException(s"Invalid State Transition from $oldValue -> $newValue, actual state was ${_inner}")
      }
    };
    def cas(t: (StateType, StateType)): Unit = cas(t._1, t._2);
    def apply(): StateType = this.synchronized { _inner };
  }
  object State {
    def init(): State = new State;
  }
}
