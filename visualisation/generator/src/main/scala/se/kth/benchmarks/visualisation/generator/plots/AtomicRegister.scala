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

object AtomicRegister {

  type Params = AtomicRegisterRequest;

  def plot(data: BenchmarkData[String]): PlotGroup = {
    // println("data: " + data);
    val paramData = {
      val bench = data.benchmark.asInstanceOf[BenchmarkWithSpace[Params]];
      val space = bench.space.asInstanceOf[ParameterSpacePB[Params]];
      val dataParams = data.mapParams(space.paramsFromCSV);
      new AtomicRegister(bench, space, dataParams)
    };

    val keysPlot = paramData.plotAlong(
      mainAxis = (params: Params) => params.numberOfKeys,
      groupings = (params: Params) => (params.readWorkload, params.writeWorkload, params.partitionSize),
      plotId = (params: (Float, Float, Int)) => s"read-${params._1}-write-${params._2}-par-${params._3}",
      plotParams = (params: (Float, Float, Int)) =>
        List(s"read workload = ${params._1}", s"write workload  = ${params._2}", s"partition size = ${params._3}"),
      plotTitle = "Execution Time",
      xAxisLabel = "number of keys",
      xAxisTitle = "Number of Keys",
      xAxisId = "number-of-keys",
      yAxisLabel = "execution time (ms)",
      units = "ms",
      calculateParams = (_params: (Float, Float, Int)) => (),
      calculateValue = (_nothing: Unit, numberOfKeys: Long, stats: Statistics) => {
        val meanTime = stats.sampleMean;
        val totalOperations = numberOfKeys.toDouble;
        val throughput = (totalOperations * 1000.0) / meanTime; // ops/s
        throughput
      },
      calculatedTitle = "Throughput",
      calculatedYAxisLabel = "avg. throughput (operations/s)",
      calculatedUnits = "operations/s"
    );
    PlotGroup.Axes(List(keysPlot))
  }

  private class AtomicRegister(_bench: BenchmarkWithSpace[Params],
                               _space: ParameterSpacePB[Params],
                               _data: BenchmarkData[Params])
      extends ExperimentPlots[Params](_bench, _space, _data);
}
