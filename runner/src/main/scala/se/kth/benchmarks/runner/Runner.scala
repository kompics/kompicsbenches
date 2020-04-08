package se.kth.benchmarks.runner

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import se.kth.benchmarks.Statistics;

import com.lkroll.common.macros.Macros
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import java.io.{File, FileWriter, PrintWriter}

object Runner {

  type Stub = BenchmarkRunnerGrpc.BenchmarkRunnerStub;

  val WAIT: Duration = 500.milliseconds;
  val MAX_RETRIES: Int = 130;
}

class Runner(conf: Conf, stub: Runner.Stub) extends LazyLogging {

  val prefix = conf.prefix();
  val testing = conf.testing();

  val sinks: List[DataSink] = {
    if (conf.console()) {
      List(new ConsoleSink(prefix))
    } else {
      val outputFolder = conf.outputFolder();
      if (!outputFolder.exists()) {
        outputFolder.mkdirs();
      }
      val outputPath = outputFolder.toPath();
      val summaryPath = outputPath.resolve("summary");
      val fullPath = outputPath.resolve("raw");
      val summaryFolder = summaryPath.toFile();
      if (!summaryFolder.exists()) {
        summaryFolder.mkdirs();
      }
      val summary = new SummarySink(prefix, summaryFolder);
      val fullFolder = fullPath.toFile();
      if (!fullFolder.exists()) {
        fullFolder.mkdirs();
      }
      val full = new FullSink(prefix, fullFolder)
      List(summary, full)
    }
  }

  def runAll(): Unit = {
    val ready = awaitReady();
    if (ready) {
      Benchmarks.benchmarks.foreach(runOne);
    }
  }

  def runOnly(benchmarks: List[String]): Unit = {
    val benchSet = benchmarks.toSet;
    val selected = Benchmarks.benchmarks.filter(bench => benchSet.contains(bench.symbol));
    if (selected.isEmpty) {
      logger.error(s"No benchmarks were selected by list ${benchmarks.mkString("[", ",", "]")}! Shutting down.");
      System.exit(1);
    } else {
      logger.info(s"Selected the following benchmarks to be run: ${selected.map(_.name).mkString("[", ", ", "]")}");
      val ready = awaitReady();
      if (ready) {
        selected.foreach(runOne);
      }
    }
  }

  def awaitReady(): Boolean = {
    var attempts = 0;
    val req = ReadyRequest();
    while (attempts < Runner.MAX_RETRIES) {
      attempts += 1;
      logger.info(s"Starting connection attempt #${attempts}.");
      val readyF = stub.ready(req);
      val result = Await.ready(readyF, Duration.Inf).value.get;
      result match {
        case Success(resp) => {
          logger.info(s"Connection attempt #${attempts} succeeded.");
          if (resp.status) {
            logger.info(s"Client is ready. Beginning benchmarks.");
            return true;
          } else {
            logger.info(s"Client is not ready, yet.");
          }
        }
        case Failure(e) => {
          logger.warn(s"Connection attempt #${attempts} failed.", e)
        }
      }
      logger.debug("Sleeping before retry.");
      Thread.sleep(Runner.WAIT.toMillis);
    }
    logger.error(s"Client was not ready within ${Runner.MAX_RETRIES} attempts. Aborting run.");
    return false;
  }

  def runOne(b: Benchmark): Unit = {
    logger.info(s"Running ${b.name}");
    val numRuns = b.requiredRuns(testing);
    b.withStub(stub, testing) { (f, p, i) =>
      logger.info(s"Awaiting run result [$i/$numRuns]...");
      val result = Await.ready(f, Duration.Inf).value.get;
      result match {
        case Success(r) => {
          r match {
            case TestResult.Empty    => logger.warn(s"Benchmark ${b.name} invocation was empty.")
            case TestFailure(reason) => logger.warn(s"Benchmark ${b.name} invocation failed: ${reason}")
            case NotImplemented()    => logger.info(s"Benchmark ${b.name} is not implemented.")
            case TestSuccess(nRuns, data) => {
              logger.info(s"Benchmark ${b.name} run [$i/$numRuns] finished successfully with ${nRuns} runs.");
              sinks.foreach(_.sink(b.symbol, p, data));
            }
          }
          logger.info(s"Benchmark ${b.name} run [$i/$numRuns] finished.");
        }
        case Failure(e) => {
          logger.warn(s"Benchmark ${b.name} run [$i/$numRuns] invocation failed.", e);
        }
      }
    }
  }
}

sealed trait RunResult;
object RunResult {
  case object Done extends RunResult;
  case object Retry extends RunResult;
}

trait DataSink {
  def sink(bench: String, params: ParameterDescription, data: Seq[Double]): Unit;
  def withWriter(target: File)(f: PrintWriter => Unit): Unit = {
    val w = new PrintWriter(target);
    try {
      f(w)
      w.flush();
    } finally {
      w.close();
    }
  }

  def withAppender(target: File)(f: PrintWriter => Unit): Unit = {
    val fw = new FileWriter(target, true);
    val w = new PrintWriter(fw);
    try {
      f(w)
      w.flush();
    } finally {
      w.close();
      fw.close();
    }
  }
}

class SummarySink(prefix: String, folder: File) extends DataSink with LazyLogging {

  val folderPath = folder.toPath();

  private def experimentFile(bench: String): File = {
    val expPath = folderPath.resolve(s"${bench}.data");
    val expFile = expPath.toFile();
    if (!expFile.exists()) {
      assert(expFile.createNewFile());
      this.withWriter(expFile) { w =>
        val row = "IMPL,PARAMS,MEAN,SSD,SEM,RSE,CI95LO,CI95UP";
        w.println(row);
      }
    }
    expFile
  }

  override def sink(bench: String, params: ParameterDescription, data: Seq[Double]): Unit = {
    val stats = new Statistics(data);
    val expFile = experimentFile(bench);
    this.withAppender(expFile) { w =>
      val ci = stats.symmetricConfidenceInterval95;
      val row =
        s"${prefix},${params.toCSV},${stats.sampleMean},${stats.sampleStandardDeviation},${stats.standardErrorOfTheMean},${stats.relativeErrorOfTheMean},${ci._1},${ci._2}";
      w.println(row);
    }
  }
}

class FullSink(prefix: String, rootFolder: File) extends DataSink with LazyLogging {

  val folder = {
    val rootPath = rootFolder.toPath();
    val expPath = rootPath.resolve(prefix);
    val expFolder = expPath.toFile();
    if (!expFolder.exists()) {
      expFolder.mkdirs();
    }
    expPath
  }

  override def sink(bench: String, params: ParameterDescription, data: Seq[Double]): Unit = {
    val f = folder.resolve(s"${bench}-${params.toPath}.data").toFile;
    if (f.createNewFile()) {
      this.withWriter(f) { w =>
        data.foreach(w.println)
      }
    } else {
      logger.error(s"Could not log results for run ${prefix}-${bench} (file could not be created)!");
      logger.info(s"Result were ${data.mkString(",")}");
    }
  }
}

class ConsoleSink(prefix: String) extends DataSink with StrictLogging {
  override def sink(bench: String, params: ParameterDescription, data: Seq[Double]): Unit = {
    val stats = new Statistics(data);
    logger.info(s"Stats:${stats.render("ms")}");
  }
}
