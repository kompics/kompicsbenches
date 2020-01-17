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
import scala.annotation.meta.param

object StreamingWindows {

  type Params = StreamingWindowsRequest;

  def plot(data: BenchmarkData[String]): PlotGroup = {
    val paramData = {
      val bench = data.benchmark.asInstanceOf[BenchmarkWithSpace[Params]];
      val space = bench.space.asInstanceOf[ParameterSpacePB[Params]];
      val dataParams = data.mapParams(space.paramsFromCSV);
      new StreamingWindows(bench, space, dataParams)
    };

    val partitionsPlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.numberOfPartitions,
      groupings = (params: Params) =>
        (params.batchSize, params.windowSize, params.numberOfWindows, params.windowSizeAmplification),
      plotId =
        (params: (Long, String, Long, Long)) => s"bs-${params._1}-ws-${params._2}-nw-${params._3}-amp-${params._4}",
      plotParams = (params: (Long, String, Long, Long)) =>
        List(s"batch size = ${params._1}",
             s"window size = ${params._2}",
             s"number of windows = ${params._3}",
             s"window size amplification = ${params._4}"),
      plotTitle = "Execution Time",
      xAxisLabel = "number of partitions",
      xAxisTitle = "Number of Partitions",
      xAxisId = "number-of-partitions",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: (Long, String, Long, Long)) => {
        params._3 // number of windows
      },
      calculateValue = (numberOfWindows: Long, numberOfPartitions: Int, stats: Statistics) => {
        val meanTime = stats.sampleMean;
        val totalWindows = numberOfWindows * numberOfPartitions;
        val windowTime = meanTime / totalWindows;
        windowTime
      },
      calculatedTitle = "Execution Time Per Window",
      calculatedYAxisLabel = "average wall time per window (ms)",
      calculatedUnits = "ms"
    );
    val windowSizePlot = paramData.plotAlong(
      mainAxis = (params: Params) => {
        val lengthSec =
          scala.concurrent.duration.Duration(params.windowSize).toUnit(java.util.concurrent.TimeUnit.SECONDS);
        val unamplifiedSize = lengthSec * 0.008; // 8kB/s
        val amplifiedSize = (params.windowSizeAmplification.toDouble * unamplifiedSize);
        (amplifiedSize * 1000.0).round
      },
      groupings = (params: Params) => (params.numberOfPartitions, params.batchSize, params.numberOfWindows),
      plotId = (params: (Int, Long, Long)) => s"np-${params._1}-bs-${params._2}-nw-${params._3}",
      plotParams = (params: (Int, Long, Long)) =>
        List(s"number of partitions = ${params._1}", s"batch size = ${params._2}", s"number of windows = ${params._3}"),
      plotTitle = "Execution Time",
      xAxisLabel = "window size (kB)",
      xAxisTitle = "Window Size",
      xAxisId = "window-size",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: (Int, Long, Long)) => {
        params._1 * params._3 // total windows
      },
      calculateValue = (totalWindows: Long, _windowSize: Long, stats: Statistics) => {
        val meanTime = stats.sampleMean;
        val windowTime = meanTime / totalWindows;
        windowTime
      },
      calculatedTitle = "Execution Time Per Window",
      calculatedYAxisLabel = "average wall time per window (ms)",
      calculatedUnits = "ms"
    );
    PlotGroup.Axes(List(partitionsPlot, windowSizePlot))
  }

  private class StreamingWindows(_bench: BenchmarkWithSpace[Params],
                                 _space: ParameterSpacePB[Params],
                                 _data: BenchmarkData[Params])
      extends ExperimentPlots[Params](_bench, _space, _data);
}
