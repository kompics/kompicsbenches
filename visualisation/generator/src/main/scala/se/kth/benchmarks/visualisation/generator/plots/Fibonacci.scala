package se.kth.benchmarks.visualisation.generator.plots

import se.kth.benchmarks.visualisation.generator.{BenchmarkData, FrameworkPlotStyle, ImplGroupedResult, JsRaw, JsString, PlotData, PlotGroup, Series}
import se.kth.benchmarks.runner.{Benchmark, BenchmarkWithSpace, ParameterSpacePB}
import kompics.benchmarks.benchmarks._

object Fibonacci {

  type Params = FibonacciRequest;

  def plot(data: BenchmarkData[String]): PlotGroup = {
    val bench = data.benchmark.asInstanceOf[BenchmarkWithSpace[Params]];
    val space = bench.space.asInstanceOf[ParameterSpacePB[Params]];
    val dataParams = data.mapParams(space.paramsFromCSV);
    val params = dataParams.paramSeries(_.fibNumber);
    val groupedSeries = dataParams.results.mapValues(_.map2D(_.fibNumber, params));
    val groupedErrorSeries = dataParams.results.mapValues(_.map2DErrorBars(_.fibNumber, params));
    val mergedSeries = (for (key <- groupedSeries.keys) yield {
      (key, groupedSeries(key), groupedErrorSeries(key))
    }).toList;
    val sortedSeries: List[Series] = mergedSeries.sortBy(_._1).map(t => List[Series](t._2, t._3)).flatten;
    val pimpedSeries: List[Series] = sortedSeries.map( series => {
      series.addMeta("tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: 'ms'}"))
        .addMeta(  "color" -> JsRaw(FrameworkPlotStyle.getColor(series.getName)))
        .addMeta(  "marker" -> JsRaw(s"{${FrameworkPlotStyle.getMarker(series.getName)}}"))
        .addMeta("dashStyle" -> JsRaw(FrameworkPlotStyle.getDashStyle(series.getName)))
    }
    );
    val paramsS = params.map(_.toString);
    val plotid = s"${bench.symbol.toLowerCase()}-fibonacci-index";
    val plot = PlotData(
      id = plotid,
      title = s"${bench.name} (Execution Time)",
      xAxisLabel = "Fibonacci index",
      xAxisCategories = paramsS,
      yAxisLabel = "total execution time (ms)",
      seriesData = pimpedSeries
    );
    PlotGroup.Single(plot)
  }
}
