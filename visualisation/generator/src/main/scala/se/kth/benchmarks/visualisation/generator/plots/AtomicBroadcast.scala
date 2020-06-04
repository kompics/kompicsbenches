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
        var normal_axis = MutTreeMap[(String, Long), TreeMap[(String, Long), ImplGroupedResult[Long]]]();
        var reconfig_axis = MutTreeMap[(String, Long, Long), TreeMap[(String, String), ImplGroupedResult[Long]]]();
        var forward_discarded_axis = MutTreeMap[(String, Long, Long), TreeMap[(String, Boolean), ImplGroupedResult[Long]]]();
        var latency_axis = MutTreeMap[(String, Long), TreeMap[String, ImplGroupedResult[Long]]]();
        for ((_impl, res) <- data.results) { // Map: KOMPACTMIX -> ImplGroupedResult
          val params = res.params;
          var normalPlots: MutTreeMap[(String, Long), MutTreeMap[(String, Long), (ListBuffer[Long], ListBuffer[Statistics])]] = MutTreeMap();
          var reconfigPlots: MutTreeMap[(String, Long, Long), MutTreeMap[(String, String), (ListBuffer[Long], ListBuffer[Statistics])]] = MutTreeMap();
          var forwardDiscardedPlots: MutTreeMap[(String, Long, Long), MutTreeMap[(String, Boolean), (ListBuffer[Long], ListBuffer[Statistics])]] = MutTreeMap();
          val latencyPlots: MutTreeMap[(String, Long), MutTreeMap[String, (ListBuffer[Long], ListBuffer[Statistics])]] = MutTreeMap();
          for ((p, stats) <- params.zip(res.stats)) { // go through each row in summary
            val reconfig = p.reconfiguration;
            reconfig match {
              case "off" => {
                val fd_plot_key = (p.reconfiguration, p.numberOfNodes, p.batchSize);
                val all_fd_series = forwardDiscardedPlots.getOrElse(fd_plot_key, MutTreeMap[(String, Boolean), (ListBuffer[Long], ListBuffer[Statistics])]());
                val fd_serie_key = (p.algorithm, p.forwardDiscarded);
                val fd_serie = all_fd_series.getOrElse(fd_serie_key, (ListBuffer[Long](), ListBuffer[Statistics]()));
                fd_serie._1 += p.numberOfProposals;
                fd_serie._2 += stats;
                all_fd_series(fd_serie_key) = fd_serie;
                forwardDiscardedPlots(fd_plot_key) = all_fd_series;
                if (!p.forwardDiscarded) {  // only put forward_discarded = false in normal plot
                  if (p.batchSize == 1) { // latency
                    val latency_plot_key = (p.reconfiguration, p.numberOfNodes);
                    val all_latency_series = latencyPlots.getOrElse(latency_plot_key, MutTreeMap[String, (ListBuffer[Long], ListBuffer[Statistics])]()); // get all series of this plot
                    val latency_serie_key = p.algorithm;
                    val latency_serie = all_latency_series.getOrElse(latency_serie_key,  (ListBuffer[Long](), ListBuffer[Statistics]()));
                    latency_serie._1 += p.numberOfProposals;
                    latency_serie._2 += stats;
                    all_latency_series(latency_serie_key) = latency_serie;
                    latencyPlots(latency_plot_key) = all_latency_series;
                  } else {
                    val normal_plot_key = (p.reconfiguration, p.numberOfNodes);
                    val all_normal_series = normalPlots.getOrElse(normal_plot_key, MutTreeMap[(String, Long), (ListBuffer[Long], ListBuffer[Statistics])]()); // get all series of this plot
                    val normal_serie_key = (p.algorithm, p.batchSize);
                    val normal_serie = all_normal_series.getOrElse(normal_serie_key, (ListBuffer[Long](), ListBuffer[Statistics]()));
                    normal_serie._1 += p.numberOfProposals;
                    normal_serie._2 += stats;
                    all_normal_series(normal_serie_key) = normal_serie;
                    normalPlots(normal_plot_key) = all_normal_series;
                  }
                }
              }
              case "single" | "majority" => {
                if (!p.forwardDiscarded) {  // TODO
                  val num_nodes = p.numberOfNodes;
                  val batch_size = p.batchSize;
                  val plot_key = (reconfig, num_nodes, batch_size);
                  var all_series = reconfigPlots.getOrElse(plot_key, MutTreeMap[(String, String), (ListBuffer[Long], ListBuffer[Statistics])]());
                  val serie_key = (p.algorithm, p.transferPolicy);
                  var serie = all_series.getOrElse(serie_key, (ListBuffer[Long](), ListBuffer[Statistics]()));
                  serie._1 += p.numberOfProposals;
                  serie._2 += stats;
                  all_series(serie_key) = serie;
                  reconfigPlots(plot_key) = all_series;
                }
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
            var all_series = TreeMap.newBuilder[(String, Long), ImplGroupedResult[Long]];
            for (serie <- normalPlot._2) {
              val algo_batchSize = serie._1;
              val num_proposals = serie._2._1.toList;
              val stats = serie._2._2.toList;
              val impl_str = s"${algo_batchSize._1}, batch_size: ${algo_batchSize._2}";
              val grouped_res = ImplGroupedResult(impl_str, num_proposals, stats);
              all_series += (algo_batchSize -> grouped_res);
            }
            normal_axis += (reconfig_numNodes -> all_series.result());
          }
          for (reconfigPlot <- reconfigPlots) {
            val reconfig_numNodes_batchSize = reconfigPlot._1;
            var all_series = TreeMap.newBuilder[(String, String), ImplGroupedResult[Long]];
            for (serie <- reconfigPlot._2) {
              val algo_transferPolicy = serie._1;
              val num_proposals = serie._2._1.toList;
              val stats = serie._2._2.toList;
              val impl_str = if (algo_transferPolicy._1 == "paxos") {
                s"${algo_transferPolicy._1}, ${algo_transferPolicy._2}"
              } else {
                "raft"
              };
              val grouped_res = ImplGroupedResult(impl_str, num_proposals, stats);
              all_series += (algo_transferPolicy -> grouped_res);
            }
            reconfig_axis += (reconfig_numNodes_batchSize -> all_series.result());
          }
          for (forwardDiscardedPlot <- forwardDiscardedPlots) {
            val reconfig_numNodes_batchSize = forwardDiscardedPlot._1;
            var all_series = TreeMap.newBuilder[(String, Boolean), ImplGroupedResult[Long]];
            for (serie <- forwardDiscardedPlot._2) {
              val algo_fd = serie._1;
              val num_proposals = serie._2._1.toList;
              val stats = serie._2._2.toList;
              val impl_str = algo_fd match {
                case ("paxos", false) => "paxos"
                case ("paxos", true) => "paxos, forward discarded"
                case ("raft", false) => "raft"
                case _ => s"${algo_fd._1}, ${algo_fd._2}"
              };
              val grouped_res = ImplGroupedResult(impl_str, num_proposals, stats);
              all_series += (algo_fd -> grouped_res);
            }
            forward_discarded_axis += (reconfig_numNodes_batchSize -> all_series.result());
          }
        }
        val normal_plotgroup = this.plotAlongPreSliced(
          mainAxis = (params: Params) => params.numberOfProposals,
          groupings = (params: Params) => (params.reconfiguration, params.numberOfNodes),
          plotId = (params: (String, Long)) => s"reconfiguration-${params._1}-num_nodes-${params._2}",
          plotParams = (params: (String, Long)) =>
            List(s"reconfiguration = ${params._1}, num_nodes = ${params._2}"),
          plotTitle = "Execution Time",
          xAxisLabel = "number of proposals",
          xAxisTitle = "Number of Proposals",
          xAxisId = "normal-number-of-proposals",
          yAxisLabel = "execution time (ms)",
          units = "ms",
          calculateParams = (_params: (String, Long)) => (),
          calculateValue = (_nothing: Unit, numberOfProposals: Long, stats: Statistics) => {
            val meanTime = stats.sampleMean;
            val totalOperations = numberOfProposals.toDouble;
            val throughput = (totalOperations * 1000.0) / meanTime; // ops/s
            throughput
          },
          calculatedTitle = "Throughput",
          calculatedYAxisLabel = "avg. throughput (operations/s)",
          calculatedUnits = "operations/s",
          seriesParams = (params: Params) => (params.algorithm, params.batchSize),
          normal_axis
        );
        val reconfig_plotgroup = this.plotAlongPreSliced(
          mainAxis = (params: Params) => params.numberOfProposals,
          groupings = (params: Params) => (params.reconfiguration, params.numberOfNodes, params.batchSize),
          plotId = (params: (String, Long, Long)) => s"reconfiguration-${params._1}-num_nodes-${params._2}-batch_size-${params._3}",
          plotParams = (params: (String, Long, Long)) =>
            List(s"reconfiguration = ${params._1}, num_nodes = ${params._2}, batch_size = ${params._3}"),
          plotTitle = "Execution Time",
          xAxisLabel = "number of proposals",
          xAxisTitle = "Reconfiguration, Number of Proposals",
          xAxisId = "reconfig-number-of-proposals",
          yAxisLabel = "execution time (ms)",
          units = "ms",
          calculateParams = (_params: (String, Long, Long)) => (),
          calculateValue = (_nothing: Unit, numberOfProposals: Long, stats: Statistics) => {
            val meanTime = stats.sampleMean;
            val totalOperations = numberOfProposals.toDouble;
            val throughput = (totalOperations * 1000.0) / meanTime; // ops/s
            throughput
          },
          calculatedTitle = "Throughput",
          calculatedYAxisLabel = "avg. throughput (operations/s)",
          calculatedUnits = "operations/s",
          seriesParams = (params: Params) => (params.algorithm, params.transferPolicy),
          reconfig_axis
        );
        val forward_discarded_plotgroup = this.plotAlongPreSliced(
          mainAxis = (params: Params) => params.numberOfProposals,
          groupings = (params: Params) => (params.reconfiguration, params.numberOfNodes, params.batchSize),
          plotId = (params: (String, Long, Long)) => s"reconfiguration-${params._1}-num_nodes-${params._2}-batch_size-${params._3}",
          plotParams = (params: (String, Long, Long)) =>
            List(s"reconfiguration = ${params._1}, num_nodes = ${params._2}, batch_size = ${params._3}"),
          plotTitle = "Execution Time",
          xAxisLabel = "number of proposals",
          xAxisTitle = "Forward discarded, Number of Proposals",
          xAxisId = "fd-number-of-proposals",
          yAxisLabel = "execution time (ms)",
          units = "ms",
          calculateParams = (_params: (String, Long, Long)) => (),
          calculateValue = (_nothing: Unit, numberOfProposals: Long, stats: Statistics) => {
            val meanTime = stats.sampleMean;
            val totalOperations = numberOfProposals.toDouble;
            val throughput = (totalOperations * 1000.0) / meanTime; // ops/s
            throughput
          },
          calculatedTitle = "Throughput",
          calculatedYAxisLabel = "avg. throughput (operations/s)",
          calculatedUnits = "operations/s",
          seriesParams = (params: Params) => (params.algorithm, params.forwardDiscarded),
          forward_discarded_axis
        );
        val latency_plotgroup = this.plotAlongPreSliced(
          mainAxis = (params: Params) => params.numberOfProposals,
          groupings = (params: Params) => (params.reconfiguration, params.numberOfNodes),
          plotId = (params: (String, Long)) => s"latency-reconfiguration-${params._1}-num_nodes-${params._2}",
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
        List(normal_plotgroup, reconfig_plotgroup, forward_discarded_plotgroup, latency_plotgroup)
      }
  }


}
