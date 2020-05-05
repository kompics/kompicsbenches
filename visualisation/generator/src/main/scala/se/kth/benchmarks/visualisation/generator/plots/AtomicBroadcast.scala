package se.kth.benchmarks.visualisation.generator.plots

import scala.reflect._
import se.kth.benchmarks.visualisation.generator.{BenchmarkData, ImplGroupedResult, JsRaw, JsString, PlotData, PlotGroup, Series, Statistics}
import se.kth.benchmarks.runner.{Benchmark, BenchmarkWithSpace, ParameterSpacePB}
import kompics.benchmarks.benchmarks._

import scala.collection.immutable.TreeMap
import scala.collection.mutable.{ListBuffer, TreeMap => MutTreeMap}

object AtomicBroadcast {

  type Params = AtomicBroadcastRequest;

  def plot(data: BenchmarkData[String]): PlotGroup = {
    println(data);
    val paramData = {
      val bench = data.benchmark.asInstanceOf[BenchmarkWithSpace[Params]];
      val space = bench.space.asInstanceOf[ParameterSpacePB[Params]];
      val dataParams = data.mapParams(space.paramsFromCSV);
      new AtomicBroadcast(bench, space, dataParams)
    };

    val plots = paramData.plot();
    println(s"Num plots: ${plots.size}");
    PlotGroup.Axes(plots)
  }

  private class AtomicBroadcast(_bench: BenchmarkWithSpace[Params],
                                _space: ParameterSpacePB[Params],
                                data: BenchmarkData[Params])
    extends ExperimentPlots[Params](_bench, _space, data) {
      def plot(): List[PlotGroup.Along] = {
        var all_plots = ListBuffer[PlotGroup.Along]();

        for ((_impl, res) <- data.results) { // Map: KOMPACTMIX -> ImplGroupedResult
          val params = res.params;
          val stats = res.stats;
          var normalPlots: MutTreeMap[String, MutTreeMap[(String, Long), (ListBuffer[Long], ListBuffer[Statistics])]] = MutTreeMap();
          for ((p, s) <- params.zip(stats)) { // go through each row in summary
            val reconfig = p.reconfiguration;
            reconfig match {
              case "off" => {
                val num_nodes = p.numberOfNodes;
                val plot_key = s"off, $num_nodes";
//                println(s"normalPlots keys: ${normalPlots.keys.mkString(", ")}");
                var all_series = normalPlots.getOrElse(plot_key, MutTreeMap[(String, Long), (ListBuffer[Long], ListBuffer[Statistics])]()); // get all series of this plot
                val serie_key = (p.algorithm, p.batchSize);
                println(s"all_series keys: ${all_series.keys.mkString((", "))}")
                var serie = all_series.getOrElse(serie_key, (ListBuffer[Long](), ListBuffer[Statistics]()));
                serie._1 += p.numberOfProposals;
                serie._2 += s;
                all_series(serie_key) = serie;
                normalPlots(plot_key) = all_series;
              }
              /*case "single" | "majority" => {}
              case _ => {}*/
            }
          }
          println(s"normalPlots: ${normalPlots.keys.mkString(", ")}");
          for (normalPlot <- normalPlots) {
            val reconfig_numNodes = normalPlot._1;
            var all_series = TreeMap.newBuilder[String, ImplGroupedResult[Long]];
//            var sliced = MutTreeMap[String, MutTreeMap[String, ImplGroupedResult[Long]]]();
            for (serie <- normalPlot._2) {
              println(s"series: ${serie._1}, params: ${serie._2._1.mkString(", ")}");
              val algo_batchSize = serie._1;
              val num_proposals = serie._2._1.toList;
              val stats = serie._2._2.toList;
              val impl = s"${algo_batchSize._1}, batch_size: ${algo_batchSize._2}";
              val impls = ImplGroupedResult(impl, num_proposals, stats);
              all_series += (impl -> impls);
//              var series = sliced.getOrElse(reconfig_numNodes, MutTreeMap[String, ImplGroupedResult[Long]]());
//              series += (impl -> impls);
//              sliced += (reconfig_numNodes -> series);
            }
            val sliced = TreeMap(reconfig_numNodes -> all_series.result());
            println(s"\nsliced: $sliced");
            val plot_group = this.plotAlongPreSliced(
              mainAxis = (params: Params) => params.numberOfProposals,
              groupings = (params: Params) => params.reconfiguration,
              plotId = (param: String) => s"reconfiguration-$param",
              plotParams = (param: String) =>
                List(s"reconfiguration = ${param}"),
              plotTitle = "Execution Time",
              xAxisLabel = "number of proposals",
              xAxisTitle = "Number of Proposals",
              xAxisId = "number-of-proposals",
              yAxisLabel = "execution time (ms)",
              units = "ms",
              calculateParams = (_params: String) => (),
              calculateValue = (_nothing: Unit, numberOfProposals: Long, stats: Statistics) => {
                val meanTime = stats.sampleMean;
                val totalOperations = numberOfProposals.toDouble;
                val throughput = (totalOperations * 1000.0) / meanTime; // ops/s
                throughput
              },
              calculatedTitle = "Throughput",
              calculatedYAxisLabel = "avg. throughput (operations/s)",
              calculatedUnits = "operations/s",
              sliced
            );
            all_plots += plot_group;

            /*
            val plots: List[PlotGroup] = sliced.toList.map {
              case (params, impls) => { // Ex: (r-50-w-50, partiion_size = 3, kompact) TODO REMOVE
                println("\nparams: " + params + ", impls: " + impls);
                val paramSeries = data.paramSeries(mainAxis);
                println("\nparamSeries: " + paramSeries.mkString(", "));
                val groupedSeries = impls.mapValues(_.map2D(identity, paramSeries));
                val groupedErrorSeries = impls.mapValues(_.map2DErrorBars(identity, paramSeries));
                groupedSeries.foreach {
                  case (key, value) => {
                    println(s"\nGROUPED_SERIES key: $key, value-data: ${value.data.mkString(", ")}")
                  }
                }
                //        println("\ngroupedseries: " + groupedSeries);
                val mergedSeries = (for (key <- groupedSeries.keys) yield {
                  (key, groupedSeries(key), groupedErrorSeries(key))
                }).toList;
                println("\nmerged series: " + mergedSeries);
                val sortedSeries: List[Series] = mergedSeries.sortBy(_._1).map(t => List[Series](t._2, t._3)).flatten;
                println("\nsorted series: " + sortedSeries);

                val pimpedSeries: List[Series] = sortedSeries.map(
                  _.addMeta(
                    "tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${units}'}")
                  )
                );
                val paramsS = paramSeries.map(_.toString);
                paramsS.foreach(x => println("paramsS: " + x));
                val corePlotId = s"${bench.symbol.toLowerCase()}-${plotId(params)}";
                val primaryPlotId = s"${corePlotId}-primary";
                val primaryPlot = PlotData(
                  id = primaryPlotId,
                  title = s"${bench.name} (${plotTitle})",
                  fixedParams = plotParams(params),
                  xAxisLabel = xAxisLabel,
                  xAxisCategories = paramsS,
                  yAxisLabel = yAxisLabel,
                  seriesData = pimpedSeries
                );
                val calculatedParams = calculateParams(params);
                val calculatedSeries = impls
                  .mapValues(_.map2DWithCalc(identity, paramSeries, (t, stats) => calculateValue(calculatedParams, t, stats)))
                  .toList;
                val sortedCalculatedSeries: List[Series] = calculatedSeries.sortBy(_._1).map(t => t._2);
                val pimpedCalculatedSeries: List[Series] = sortedCalculatedSeries.map(
                  _.addMeta(
                    "tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${calculatedUnits}'}")
                  )
                );
                val calculatedPlotId = s"${corePlotId}-calculated";
                val calculatedPlot = PlotData(
                  id = calculatedPlotId,
                  title = s"${bench.name} (${calculatedTitle})",
                  fixedParams = plotParams(params),
                  xAxisLabel = xAxisLabel,
                  xAxisCategories = paramsS,
                  yAxisLabel = calculatedYAxisLabel,
                  seriesData = pimpedCalculatedSeries
                );
                PlotGroup.CalculatedWithParameters(primaryPlot, calculatedPlot)
              }
            };
            */

          }
        }
        all_plots.toList
//        PlotGroup.Along(xAxisId, xAxisTitle, plots)
      }
  }


}
