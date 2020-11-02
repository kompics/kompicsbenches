package se.kth.benchmarks.visualisation.generator.plots

import scala.reflect._
import se.kth.benchmarks.visualisation.generator.{
  BenchmarkData,
  ImplGroupedResult,
  JsRaw,
  JsString,
  PlotData,
  PlotGroup,
  Series,
  Statistics
}
import se.kth.benchmarks.runner.{Benchmark, BenchmarkWithSpace, ParameterSpacePB}
import kompics.benchmarks.benchmarks._

object SizedThroughput {

  type Params = SizedThroughputRequest;

  def plot(data: BenchmarkData[String]): PlotGroup = {
    val paramData = {
      val bench = data.benchmark.asInstanceOf[BenchmarkWithSpace[Params]];
      val space = bench.space.asInstanceOf[ParameterSpacePB[Params]];
      val dataParams = data.mapParams(space.paramsFromCSV);
      new SizedThroughput(bench, space, dataParams)
    };
    // Plots throughput dependent on messages size
    val messageSizePlot = paramData.plotAlong(
      // params: message_size: 10!N! batch_size: 10!N! number_of_batches: 4!N! number_of_pairs: 4!
      // SizedThroughputRequest(messageSize, batchSize, numberOfBatches, numberOfPairs)
      mainAxis = (params: Params) => params.messageSize,
      groupings = (params: Params) => (params.numberOfBatches, params.numberOfPairs, params.batchSize),
      plotId = (params: (Int, Int, Int)) => s"nob-${params._1}-nop-${params._2}-bs-${params._3}",
      plotParams = (params: (Int, Int, Int)) =>
        List(s"number of batches = ${params._1}",
          s"number of pairs = ${params._2}",
          s"batch size = ${params._3})"),
      plotTitle = "Execution Time",
      xAxisLabel = "message size (bytes)",
      xAxisTitle = "Message Size",
      xAxisId = "message-size",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: (Int, Int, Int)) => {
        params._1.toLong * params._2.toLong * params._3.toLong // total messages
      },
      calculateValue = (totalMessages: Long, messageSize: Int, stats: Statistics) => {
        val totalBytes = totalMessages.toLong * messageSize.toLong;
        val meanTime = stats.sampleMean.toLong;
        val throughput = (totalBytes.toDouble) / (meanTime * 1000); // MB/s (1000 to get from B/ms to MB/s )
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "throughput (MB/s)",
      calculatedUnits = "MB/s"
    );
    // Plots throughput by parallelism, finds parallellisms effect on the throughput
    val parallelismPlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.numberOfPairs,
      groupings = (params: Params) => (params.messageSize, params.batchSize, params.numberOfBatches),
      plotId = (params: (Int, Int, Int)) => s"ms-${params._1}-bs-${params._2}-nob-${params._3}",
      plotParams = (params: (Int, Int, Int)) =>
        List(s"Message Size (bytes) = ${params._1}" +
          s"", s"Messages per batch = ${params._2}" +
          s"", s"Number of Batches = ${params._3})"),
      plotTitle = "Execution Time",
      xAxisLabel = "parallelism (number of pairs)",
      xAxisTitle = "Parallelism",
      xAxisId = "parallelism",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: (Int, Int, Int)) => {
        params._1.toLong * params._2.toLong * params._3.toLong // total bytes per pair
      },
      calculateValue = (totalMessagesPerPair: Long, pairs: Int, stats: Statistics) => {
        val totalBytes = totalMessagesPerPair * pairs.toLong;
        val meanTime = stats.sampleMean;
        val throughput = (totalBytes.toLong) / (meanTime * 1000); // MB/s (1000 to get from B/ms to MB/s )
        // println("pairs: " + pairs + ", totalBytes: " + totalBytes + ", throughput: " + throughput);
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "throughput (MB/s)",
      calculatedUnits = "MB/s"
    );
    /* Plots throughput by how long the experiment runs. To find converging throughput.
    val batchCountPlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.numberOfBatches,
      groupings = (params: Params) => (params.messageSize, params.numberOfPairs, params.batchSize),
      plotId = (params: (Int, Int, Int)) => s"np-${params._1}-ps-${params._2}-static-${params._3}",
      plotParams = (params: (Int, Int, Int)) =>
        List(s"Message Size (bytes) = ${params._1}", s"Number of Pairs = ${params._2}", s"Batch Size = ${params._3})"),
      plotTitle = "Execution Time",
      xAxisLabel = "number of batches per pair",
      xAxisTitle = "Number of Batches",
      xAxisId = "number-of-batches",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: (Int, Int, Int)) => {
        params._1 * params._2 * params._3 // bytes per batch
      },
      calculateValue = (bytesPerBatch: Long, numberOfBatches: Long, stats: Statistics) => {
        val totalBytes = bytesPerBatch * numberOfBatches;
        val meanTime = stats.sampleMean;
        val throughput = (totalBytes.toDouble) / (meanTime * 1000); // MB/s (1000 to get from B/ms to MB/s )
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "throughput (MB/s)",
      calculatedUnits = "MB/s"
    );*/
    // Plots throughput dependent on messages per batch
    val pipelinePlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.batchSize,
      groupings = (params: Params) => (params.numberOfBatches, params.numberOfPairs, params.messageSize),
      plotId = (params: (Int, Int, Int)) => s"nob-${params._1}-nop-${params._2}-ms-${params._3}",
      plotParams = (params: (Int, Int, Int)) =>
        List(s"number of batches = ${params._1}",
          s"number of pairs = ${params._2}",
          s"message size = ${params._3})"),
      plotTitle = "Execution Time",
      xAxisLabel = "batch size",
      xAxisTitle = "Batch Size",
      xAxisId = "batch-size",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: (Int, Int, Int)) => {
        params._1.toLong * params._2.toLong * params._3.toLong // aggregate=(bytes per run/batchSize)
      },
      calculateValue = (aggregate: Long, batchSize: Int, stats: Statistics) => {
        val totalBytes = aggregate.toLong * batchSize.toLong;
        val meanTime = stats.sampleMean.toLong;
        val throughput = (totalBytes.toDouble) / (meanTime * 1000); // MB/s (1000 to get from B/ms to MB/s )
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "throughput (MB/s)",
      calculatedUnits = "MB/s"
    );
    PlotGroup.Axes(List(parallelismPlot, pipelinePlot, messageSizePlot))
  }
  private class SizedThroughput(_bench: BenchmarkWithSpace[Params],
                                   _space: ParameterSpacePB[Params],
                                   _data: BenchmarkData[Params])
      extends ExperimentPlots[Params](_bench, _space, _data);
}