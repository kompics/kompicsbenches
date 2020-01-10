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

object ThroughputPingPong {

  type Params = ThroughputPingPongRequest;

  def plot(data: BenchmarkData[String]): PlotGroup = {
    val paramData = {
      val bench = data.benchmark.asInstanceOf[BenchmarkWithSpace[Params]];
      val space = bench.space.asInstanceOf[ParameterSpacePB[Params]];
      val dataParams = data.mapParams(space.paramsFromCSV);
      new ThroughputPingPong(bench, space, dataParams)
    };

    val parallelismPlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.parallelism,
      groupings = (params: Params) => (params.messagesPerPair, params.pipelineSize, params.staticOnly),
      plotId = (params: (Long, Long, Boolean)) => s"mpp-${params._1}-ps-${params._2}-static-${params._3}",
      plotParams = (params: (Long, Long, Boolean)) =>
        List(s"number of messages per pair = ${params._1}", s"pipeline size = ${params._2}", s"static = ${params._3})"),
      plotTitle = "Execution Time",
      xAxisLabel = "parallelism (number of pairs)",
      xAxisTitle = "Parallelism",
      xAxisId = "parallelism",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: (Long, Long, Boolean)) => {
        params._1 * 2 // total messages per pair
      },
      calculateValue = (totalMessagesPerPair: Long, pairs: Int, stats: Statistics) => {
        val totalMessages = totalMessagesPerPair * pairs;
        val meanTime = stats.sampleMean;
        val throughput = (totalMessages.toDouble * 1000.0) / meanTime; // msgs/s (1000 to get from ms to s)
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "throughput (msgs/s)",
      calculatedUnits = "msgs/s"
    );
    val messagesPlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.messagesPerPair,
      groupings = (params: Params) => (params.parallelism, params.pipelineSize, params.staticOnly),
      plotId = (params: (Int, Long, Boolean)) => s"np-${params._1}-ps-${params._2}-static-${params._3}",
      plotParams = (params: (Int, Long, Boolean)) =>
        List(s"number of pairs = ${params._1}", s"pipeline size = ${params._2}", s"static = ${params._3})"),
      plotTitle = "Execution Time",
      xAxisLabel = "number of messages per pair",
      xAxisTitle = "Number of Messages",
      xAxisId = "number-of-messages",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: (Int, Long, Boolean)) => {
        params._1.toLong * 2 // double pairs
      },
      calculateValue = (doublePairs: Long, messagesPerPair: Long, stats: Statistics) => {
        val totalMessages = doublePairs * messagesPerPair;
        val meanTime = stats.sampleMean;
        val throughput = (totalMessages.toDouble * 1000.0) / meanTime; // msgs/s (1000 to get from ms to s)
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "throughput (msgs/s)",
      calculatedUnits = "msgs/s"
    );
    val pipelinePlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.pipelineSize,
      groupings = (params: Params) => (params.messagesPerPair, params.parallelism, params.staticOnly),
      plotId = (params: (Long, Int, Boolean)) => s"mpp-${params._1}-np-${params._2}-static-${params._3}",
      plotParams = (params: (Long, Int, Boolean)) =>
        List(s"number of messages per pair = ${params._1}",
             s"number of pairs = ${params._2}",
             s"static = ${params._3})"),
      plotTitle = "Execution Time",
      xAxisLabel = "pipelining depth",
      xAxisTitle = "Pipelining Depth",
      xAxisId = "pipeline-size",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: (Long, Int, Boolean)) => {
        params._1 * params._2.toLong * 2 // total messages
      },
      calculateValue = (totalMessages: Long, _: Long, stats: Statistics) => {
        val meanTime = stats.sampleMean;
        val throughput = (totalMessages.toDouble * 1000.0) / meanTime; // msgs/s (1000 to get from ms to s)
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "throughput (msgs/s)",
      calculatedUnits = "msgs/s"
    );
    PlotGroup.Axes(List(parallelismPlot, messagesPlot, pipelinePlot))
  }
  private class ThroughputPingPong(_bench: BenchmarkWithSpace[Params],
                                   _space: ParameterSpacePB[Params],
                                   _data: BenchmarkData[Params])
      extends ExperimentPlots[Params](_bench, _space, _data);

}
