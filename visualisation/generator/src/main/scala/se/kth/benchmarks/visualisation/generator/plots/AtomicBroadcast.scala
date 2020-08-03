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
    val paramData = {
      val bench = data.benchmark.asInstanceOf[BenchmarkWithSpace[Params]];
      val space = bench.space.asInstanceOf[ParameterSpacePB[Params]];
      val dataParams = data.mapParams(space.paramsFromCSV);
      new AtomicBroadcast(bench, space, dataParams)
    };

    val plots = paramData.plot();
    PlotGroup.Axes(plots)
  }

  private class AtomicBroadcast(_bench: BenchmarkWithSpace[Params],
                                _space: ParameterSpacePB[Params],
                                data: BenchmarkData[Params])
    extends ExperimentPlots[Params](_bench, _space, data) {
      def plot(): List[PlotGroup.Along] = {
        var normal_axis = MutTreeMap[(String, Long, Long), TreeMap[String, ImplGroupedResult[Long]]]();
        var reconfig_axis = MutTreeMap[(String, Long, Long), TreeMap[(String, String), ImplGroupedResult[Long]]]();
        var latency_axis = MutTreeMap[(String, Long), TreeMap[String, ImplGroupedResult[Long]]]();
        for ((_impl, res) <- data.results) { // Map: KOMPACTMIX -> ImplGroupedResult
          val params = res.params;
          val normalPlots: MutTreeMap[(String, Long, Long), MutTreeMap[String, (ListBuffer[Long], ListBuffer[Statistics])]] = MutTreeMap();
          val reconfigPlots: MutTreeMap[(String, Long, Long), MutTreeMap[(String, String), (ListBuffer[Long], ListBuffer[Statistics])]] = MutTreeMap();
          val latencyPlots: MutTreeMap[(String, Long), MutTreeMap[String, (ListBuffer[Long], ListBuffer[Statistics])]] = MutTreeMap();
          for ((p, stats) <- params.zip(res.stats)) { // go through each row in summary
            val reconfig = p.reconfiguration;
            reconfig match {
              case "off" => {
                if (p.concurrentProposals == 1) { // latency
                  val latency_plot_key = (p.reconfiguration, p.numberOfNodes);
                  val all_latency_series = latencyPlots.getOrElse(latency_plot_key, MutTreeMap[String, (ListBuffer[Long], ListBuffer[Statistics])]()); // get all series of this plot
                  val latency_serie_key = p.algorithm;
                  val latency_serie = all_latency_series.getOrElse(latency_serie_key,  (ListBuffer[Long](), ListBuffer[Statistics]()));
                  latency_serie._1 += p.numberOfProposals;
                  latency_serie._2 += stats;
                  all_latency_series(latency_serie_key) = latency_serie;
                  latencyPlots(latency_plot_key) = all_latency_series;
                } else {
                  val normal_plot_key = (p.reconfiguration, p.numberOfNodes, p.numberOfProposals);
                  val all_normal_series = normalPlots.getOrElse(normal_plot_key, MutTreeMap[String, (ListBuffer[Long], ListBuffer[Statistics])]()); // get all series of this plot
                  val normal_serie_key = p.algorithm;
                  val normal_serie = all_normal_series.getOrElse(normal_serie_key, (ListBuffer[Long](), ListBuffer[Statistics]()));
                  normal_serie._1 += p.concurrentProposals;
                  normal_serie._2 += stats;
                  all_normal_series(normal_serie_key) = normal_serie;
                  normalPlots(normal_plot_key) = all_normal_series;
                }
              }
              case "single" | "majority" => {
                val plot_key = (reconfig, p.numberOfNodes, p.numberOfProposals);
                val all_series = reconfigPlots.getOrElse(plot_key, MutTreeMap[(String, String), (ListBuffer[Long], ListBuffer[Statistics])]());
                val serie_key = (p.algorithm, p.reconfigPolicy);
                val serie = all_series.getOrElse(serie_key, (ListBuffer[Long](), ListBuffer[Statistics]()));
                serie._1 += p.concurrentProposals;
                serie._2 += stats;
                all_series(serie_key) = serie;
                reconfigPlots(plot_key) = all_series;
              }
            }
          }
          for (latencyPlot <- latencyPlots) {
            val reconfig_numNodes = latencyPlot._1;
            var all_series = TreeMap.newBuilder[String, ImplGroupedResult[Long]];
            for (serie <- latencyPlot._2) {
              val algorithm = serie._1;
              val num_proposals = serie._2._1.toList;
              val stats = serie._2._2.toList;
              val grouped_res = ImplGroupedResult(algorithm, num_proposals, stats);
              all_series += (algorithm -> grouped_res);
            }
            latency_axis += (reconfig_numNodes -> all_series.result());
          }
          for (normalPlot <- normalPlots) {
            val reconfig_numNodes = normalPlot._1;
            var all_series = TreeMap.newBuilder[String, ImplGroupedResult[Long]];
            for (serie <- normalPlot._2) {
              val algorithm = serie._1;
              val num_proposals = serie._2._1.toList;
              val stats = serie._2._2.toList;
              val grouped_res = ImplGroupedResult(algorithm, num_proposals, stats);
              all_series += (algorithm -> grouped_res);
            }
            normal_axis += (reconfig_numNodes -> all_series.result());
          }
          for (reconfigPlot <- reconfigPlots) {
            val reconfig_numNodes_concurrentProposals = reconfigPlot._1;
            var all_series = TreeMap.newBuilder[(String, String), ImplGroupedResult[Long]];
            for (serie <- reconfigPlot._2) {
              val algo_reconfigPolicy = serie._1;
              val num_proposals = serie._2._1.toList;
              val stats = serie._2._2.toList;
              val impl_str = if (algo_reconfigPolicy._1 == "raft" && algo_reconfigPolicy._2 == "none") {
                "raft"
              } else {
                s"${algo_reconfigPolicy._1}, ${algo_reconfigPolicy._2}"
              };
              val grouped_res = ImplGroupedResult(impl_str, num_proposals, stats);
              all_series += (algo_reconfigPolicy -> grouped_res);
            }
            reconfig_axis += (reconfig_numNodes_concurrentProposals -> all_series.result());
          }
        }
        var all_plotgroups: ListBuffer[PlotGroup.Along] = ListBuffer();
        if (normal_axis.nonEmpty) {
          val normal_plotgroup = this.plotAlongPreSliced(
            mainAxis = (params: Params) => params.concurrentProposals,
            groupings = (params: Params) => (params.reconfiguration, params.numberOfNodes, params.numberOfProposals),
            plotId = (params: (String, Long, Long)) => s"reconfiguration-${params._1}-num_nodes-${params._2}-num_proposals-${params._3}",
            plotParams = (params: (String, Long, Long)) =>
              List(s"reconfiguration = ${params._1}, num_nodes = ${params._2}, num_proposals = ${params._3}"),
            plotTitle = "Execution Time",
            xAxisLabel = "number of concurrent proposals",
            xAxisTitle = "Number of Concurrent Proposals",
            xAxisId = "normal-number-of-concurrent-proposals",
            yAxisLabel = "execution time (ms)",
            units = "ms",
            calculateParams = (params: (String, Long, Long)) => params._3,
            calculateValue = (numberOfProposals: Long, _concurrentProposals: Long, stats: Statistics) => {
              val meanTime = stats.sampleMean;
              val totalOperations = numberOfProposals.toDouble;
              val throughput = (totalOperations * 1000.0) / meanTime; // ops/s
              throughput
            },
            calculatedTitle = "Throughput",
            calculatedYAxisLabel = "avg. throughput (operations/s)",
            calculatedUnits = "operations/s",
            seriesParams = (params: Params) => params.algorithm,
            normal_axis
          );
          all_plotgroups += normal_plotgroup;
        }
        if (reconfig_axis.nonEmpty) {
          val reconfig_plotgroup = this.plotAlongPreSliced(
            mainAxis = (params: Params) => params.concurrentProposals,
            groupings = (params: Params) => (params.reconfiguration, params.numberOfNodes, params.concurrentProposals),
            plotId = (params: (String, Long, Long)) => s"reconfiguration-${params._1}-num_nodes-${params._2}-num_proposals-${params._3}",
            plotParams = (params: (String, Long, Long)) =>
              List(s"reconfiguration = ${params._1}, num_nodes = ${params._2}, num_proposals = ${params._3}"),
            plotTitle = "Execution Time",
            xAxisLabel = "number of concurrent proposals",
            xAxisTitle = "Reconfiguration, Number of Concurrent Proposals",
            xAxisId = "reconfig-number-of-concurrent-proposals",
            yAxisLabel = "execution time (ms)",
            units = "ms",
            calculateParams = (params: (String, Long, Long)) => params._3,
            calculateValue = (numberOfProposals: Long, _concurrentProposals: Long, stats: Statistics) => {
              val meanTime = stats.sampleMean;
              val totalOperations = numberOfProposals.toDouble;
              val throughput = (totalOperations * 1000.0) / meanTime; // ops/s
              throughput
            },
            calculatedTitle = "Throughput",
            calculatedYAxisLabel = "avg. throughput (operations/s)",
            calculatedUnits = "operations/s",
            seriesParams = (params: Params) => (params.algorithm, params.reconfigPolicy),
            reconfig_axis
          );
          all_plotgroups += reconfig_plotgroup
        }
        if (latency_axis.nonEmpty) {
          val latency_plotgroup = this.plotAlongPreSliced(
            mainAxis = (params: Params) => params.numberOfProposals,
            groupings = (params: Params) => (params.reconfiguration, params.numberOfNodes),
            plotId = (params: (String, Long)) => s"latency-${params._1}-num_nodes-${params._2}",
            plotParams = (params: (String, Long)) =>
              List(s"reconfiguration = ${params._1}, num_nodes = ${params._2}"),
            plotTitle = "Execution Time",
            xAxisLabel = "number of proposals",
            xAxisTitle = "Latency, Number of Proposals",
            xAxisId = "latency-number-of-proposals",
            yAxisLabel = "execution time (ms)",
            units = "ms",
            calculateParams = (_params: (String, Long)) => (),
            calculateValue = (_nothing: Unit, numberOfProposals: Long, stats: Statistics) => {
              val meanTime = stats.sampleMean;
              val totalOperations = numberOfProposals.toDouble;
              val latency = meanTime / totalOperations;
              latency
            },
            calculatedTitle = "Latency",
            calculatedYAxisLabel = "avg. latency (ms/operation)",
            calculatedUnits = "ms/operation",
            seriesParams = (params: Params) => params.algorithm,
            latency_axis
          );
          all_plotgroups += latency_plotgroup
        }
        all_plotgroups.toList
      }
  }


}
