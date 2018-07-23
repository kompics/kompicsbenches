package se.kth.benchmarks.runner

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{ Future, ExecutionContext, Await }
import scala.concurrent.duration.Duration
import scala.util.{ Try, Success, Failure }
import se.kth.benchmarks.Statistics;

import com.lkroll.common.macros.Macros
import com.typesafe.scalalogging.{ LazyLogging, StrictLogging }
import java.io.{ File, PrintWriter, FileWriter }

class Runner(conf: Conf, stub: Runner.Stub) extends LazyLogging {
  import TestResultMessage.SealedValue;

  val prefix = conf.prefix();

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
    Benchmarks.benchmarks.foreach(runOne)
  }

  def runOne(b: Benchmark): Unit = {
    logger.info(s"Running ${b.name}");
    b.withStub(stub) { (f, p) =>
      logger.info(s"Awaiting run result...");
      val result = Await.ready(f, Duration.Inf).value.get;
      result match {
        case Success(r) => {
          r.sealedValue match {
            case SealedValue.Empty             => logger.warn(s"Benchmark ${b.name} invocation was empty.")
            case SealedValue.Failure(msg)      => logger.warn(s"Benchmark ${b.name} invocation failed: ${msg.reason}")
            case SealedValue.NotImplemented(_) => logger.info(s"Benchmark ${b.name} is not implemented.")
            case SealedValue.Success(TestSuccess(nRuns, data)) => {
              logger.info(s"Benchmark ${b.name} finished successfully with ${nRuns} runs.");
              sinks.foreach(_.sink(b.symbol, p, data));
            }
          }
          logger.info(s"Benchmark ${b.name} finished successfully.");
        }
        case Failure(e) => {
          logger.warn(s"Benchmark ${b.name} invocation failed.", e);
        }
      }
    }
  }
}

object Runner {
  type Stub = BenchmarkRunnerGrpc.BenchmarkRunnerStub;
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
      val row = s"${prefix},${params.toCSV},${stats.sampleMean},${stats.sampleStandardDeviation},${stats.standardErrorOfTheMean},${stats.relativeErrorOfTheMean},${ci._1},${ci._2}";
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
    val f = folder.resolve(s"${bench}-${params.toSuffix}.data").toFile;
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
