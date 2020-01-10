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

object Chameneos {

  type Params = ChameneosRequest;

  def plot(data: BenchmarkData[String]): PlotGroup = {
    val paramData = {
      val bench = data.benchmark.asInstanceOf[BenchmarkWithSpace[Params]];
      val space = bench.space.asInstanceOf[ParameterSpacePB[Params]];
      val dataParams = data.mapParams(space.paramsFromCSV);
      new Chameneos(bench, space, dataParams)
    };

    val meetingsPlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.numberOfMeetings,
      groupings = (params: Params) => params.numberOfChameneos,
      plotId = (params: Int) => s"cham-${params}",
      plotParams = (params: Int) => List(s"number of chameneos = $params"),
      plotTitle = "Execution Time",
      xAxisLabel = "number of meetings",
      xAxisTitle = "Number of Meetings",
      xAxisId = "number-of-meetings",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: Int) => {
        params // number of chameneos
      },
      calculateValue = (numberOfChameneos: Int, numberOfMeetings: Long, stats: Statistics) => {
        val meanTime = stats.sampleMean;
        val chameneos = numberOfChameneos.toDouble;
        val throughput = (numberOfMeetings.toDouble * 1000.0) / (meanTime * chameneos); // meetings/s per chameneo
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "avg. throughput (meetings/s) per chameneo",
      calculatedUnits = "meetings/s"
    );
    val chameneosPlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.numberOfChameneos,
      groupings = (params: Params) => params.numberOfMeetings,
      plotId = (params: Long) => s"nm-${params}",
      plotParams = (params: Long) => List(s"number of meetings = $params"),
      plotTitle = "Execution Time",
      xAxisLabel = "number of chameneos",
      xAxisTitle = "Number of Chameneos",
      xAxisId = "number-of-chameneos",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (params: Long) => {
        params // number of chameneos
      },
      calculateValue = (numberOfMeetings: Long, numberOfChameneos: Int, stats: Statistics) => {
        val meanTime = stats.sampleMean;
        val chameneos = numberOfChameneos.toDouble;
        val throughput = (numberOfMeetings.toDouble * 1000.0) / (meanTime * chameneos); // meetings/s per chameneo
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "avg. throughput (meetings/s) per chameneo",
      calculatedUnits = "meetings/s"
    );
    PlotGroup.Axes(List(chameneosPlot, meetingsPlot))
  }

  private class Chameneos(_bench: BenchmarkWithSpace[Params],
                          _space: ParameterSpacePB[Params],
                          _data: BenchmarkData[Params])
      extends ExperimentPlots[Params](_bench, _space, _data);
}
