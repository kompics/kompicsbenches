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

object AllPairsShortestPath {

  type Params = APSPRequest;

  def plot(data: BenchmarkData[String]): PlotGroup = {
    val paramData = {
      val bench = data.benchmark.asInstanceOf[BenchmarkWithSpace[Params]];
      val space = bench.space.asInstanceOf[ParameterSpacePB[Params]];
      val dataParams = data.mapParams(space.paramsFromCSV);
      new AllPairsShortestPath(bench, space, dataParams)
    };

    val nodesPlot = paramData.plotAlongNoCalc(
      mainAxis = (params: Params) => params.numberOfNodes,
      groupings = (params: Params) => params.blockSize,
      plotId = (params: Int) => s"bs-${params}",
      plotParams = (params: Int) => List(s"block size = $params"),
      plotTitle = "Execution Time",
      xAxisLabel = "number of nodes (|V|)",
      xAxisTitle = "Number of Nodes",
      xAxisId = "number-of-nodes",
      yAxisLabel = "execution time (ms)",
      units = "ms"
    );
    val workersPlot = paramData.plotAlongNoCalc(
      //mainAxis = (params: Params) => params.blockSize,
      mainAxis = (params: Params) => {
        val entriesSingleDim = params.numberOfNodes / params.blockSize;
        val totalWorkers = entriesSingleDim * entriesSingleDim;
        totalWorkers
      },
      groupings = (params: Params) => params.numberOfNodes,
      plotId = (params: Int) => s"nodes-${params}",
      plotParams = (params: Int) => List(s"number of nodes = $params"),
      plotTitle = "Execution Time",
      xAxisLabel = "number of blocks (|V|/|B|)^2",
      xAxisTitle = "Number of Blocks",
      xAxisId = "number-of-blocks",
      yAxisLabel = "execution time (ms)",
      units = "ms"
    );
    PlotGroup.Axes(List(nodesPlot, workersPlot))
  }

  private class AllPairsShortestPath(_bench: BenchmarkWithSpace[Params],
                                     _space: ParameterSpacePB[Params],
                                     _data: BenchmarkData[Params])
      extends ExperimentPlots[Params](_bench, _space, _data);
}
