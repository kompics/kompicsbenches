#!/usr/bin/env amm

interp.repositories() ++= Seq(coursier.MavenRepository(
    "https://dl.bintray.com/lkrollcom/maven"
))

@

import ammonite.ops._
import ammonite.ops.ImplicitWd._
//import $ivy.`com.lihaoyi::scalatags:0.6.2`, scalatags.Text.all._
//import $ivy.`org.sameersingh::scalaplot:0.0.4`, org.sameersingh.scalaplot.Implicits._
import $ivy.`com.panayotis.javaplot:javaplot:0.5.0`, com.panayotis.gnuplot.{plot => gplot, utils => gutils, _}
import $ivy.`com.github.tototoshi::scala-csv:1.3.5`, com.github.tototoshi.csv._
import $file.build
import java.io.File
import $file.benchmarks, benchmarks.{BenchmarkImpl, implementations}
import $ivy.`se.kth.benchmarks::benchmark-suite-runner:0.2.0-SNAPSHOT`, se.kth.benchmarks.runner._, se.kth.benchmarks.runner.utils._, kompics.benchmarks.benchmarks._
//import build.{relps, relp, binp}

val results = pwd / 'results;
val plots = pwd / 'plots;

@main
def main(show: Boolean = false): Unit = {
	if (!(exists! results)) {
		println("No results to plot.");
		return;
	}
	if (exists! plots) {
		rm! plots;
	}
	mkdir! plots;
	(ls! results).foreach(plotRun(_, show));
}

private def plotRun(run: Path, show: Boolean): Unit = {
	println(s"Plotting Run '${run}'");
	val summary = run / 'summary;
	val output = plots / run.last;
	mkdir! output;
	(ls! summary).filter(_.toString.endsWith(".data")).foreach(plotData(_, output, show));
}

private def plotData(data: Path, output: Path, show: Boolean): Unit = {
	println(s"Plotting Data '${data}'");
	print("Loading data...");
	//val rawData = read.lines! data;
	val reader = CSVReader.open(data.toIO);
	val rawData = reader.allWithHeaders();
	reader.close()
	println("done");
	//println(rawData);
	val mean = rawData.map(m => (m("IMPL"), m("PARAMS"), m("MEAN").toDouble));
	val meanGrouped = mean.groupBy(_._1).map { case (key, entries) =>
		val params = entries.map(_._2);
		val means = entries.map(_._3);
		(key -> ImplGroupedResult(implementations(key).label, params, means))
	};
	//println(meanGrouped);
	val fileName = data.last.toString.replace(".data", "");
	val benchO = Benchmarks.benchmarkLookup.get(fileName);
	benchO match {
		case Some(bench) => {
			fileName match {
				case "PINGPONG"|"NETPINGPONG" => plotBenchPP(bench, meanGrouped, output, show)
				case "TPPINGPONG"|"NETTPPINGPONG" => plotBenchTPPP(bench, meanGrouped, output, show)
				case symbol => println(s"Could not find instructions for '${symbol}'! Skipping plot.")
			}
		}
		case None => {
			println(s"Could not get benchmark entry for symbol '${fileName}'! Skipping plot.");
		}
	}
	println(s"Finished with '${data}'");
}

private def plotBenchPP(b: Benchmark, res: Map[String, ImplGroupedResult[String]], output: Path, show: Boolean): Unit = {
	val bench = b.asInstanceOf[BenchmarkWithSpace[PingPongRequest]];
	val space = bench.space.asInstanceOf[ParameterSpacePB[PingPongRequest]];
	val meanGroupedParams = res.mapValues(_.mapParams(space.paramsFromCSV));
	val meanGroupedLong = meanGroupedParams.mapValues(_.map2D(_.numberOfMessages));
	val p = new JavaPlot();
	if (!show) {
		val outfile = output / s"${b.symbol}.eps";
		val epsf = new terminal.PostscriptTerminal(outfile.toString);
		epsf.setColor(true);
        p.setTerminal(epsf);
	}
	p.getAxis("x").setLabel("#msgs");
	p.getAxis("y").setLabel("execution time (ms)");
	p.setTitle(b.name);
	meanGroupedLong.foreach { case (key, res) =>
		val dsp = new gplot.DataSetPlot(res);
		dsp.setTitle(res.implLabel);
		val ps = new style.PlotStyle(style.Style.LINESPOINTS);
		val colour = colourMap(key);
		ps.setLineType(colour);
		ps.setPointType(pointMap(key));
		dsp.setPlotStyle(ps);
		p.addPlot(dsp);
	}
	p.plot();
}

private def plotBenchTPPP(b: Benchmark, res: Map[String, ImplGroupedResult[String]], output: Path, show: Boolean): Unit = {
	val bench = b.asInstanceOf[BenchmarkWithSpace[ThroughputPingPongRequest]];
	val space = bench.space.asInstanceOf[ParameterSpacePB[ThroughputPingPongRequest]];
	val meanGroupedParamsTime = res.mapValues(_.mapParams(space.paramsFromCSV));
	val meanGroupedParams = ImplResults.mapData(
		meanGroupedParamsTime,
		(req: ThroughputPingPongRequest, meanTime: Double) => {
			val totalMessages = req.messagesPerPair * req.parallelism * 2; // 1 Ping  and 1 Pong per exchange
			val throughput = (totalMessages.toDouble*1000.0)/meanTime; // msgs/s (1000 to get from ms to s)
			throughput
		}
	);
	{
		val meanGroupedPipelining = ImplResults.slices(
			meanGroupedParams, 
			(req: ThroughputPingPongRequest) => (req.messagesPerPair, req.pipelineSize, req.staticOnly),
			(req: ThroughputPingPongRequest) => req.parallelism
		);
		meanGroupedPipelining.foreach { case (params, impls) =>
			val p = new JavaPlot();
			if (!show) {
				val outfile = output / s"${b.symbol}-msgs${params._1}-pipe${params._2}-static${params._3}.eps";
				val epsf = new terminal.PostscriptTerminal(outfile.toString);
				epsf.setColor(true);
		        p.setTerminal(epsf);
			}
			p.getAxis("x").setLabel("parallelism (#pairs)");
			p.getAxis("y").setLabel("throughput (msgs/s)");
			p.setTitle(s"${b.name} (#msgs/pair = ${params._1}, #pipeline size = ${params._2}, static = ${params._3})");
			impls.foreach { case (key, res) =>
				val dsp = new gplot.DataSetPlot(res);
				dsp.setTitle(res.implLabel);
				val ps = new style.PlotStyle(style.Style.LINESPOINTS);
				val colour = colourMap(key);
				ps.setLineType(colour);
				ps.setPointType(pointMap(key));
				dsp.setPlotStyle(ps);
				p.addPlot(dsp);
			}
			p.plot();
		}
	}
	{
		val meanGroupedPipelining = ImplResults.slices(
			meanGroupedParams, 
			(req: ThroughputPingPongRequest) => (req.parallelism, req.pipelineSize, req.staticOnly),
			(req: ThroughputPingPongRequest) => req.messagesPerPair
		);
		meanGroupedPipelining.foreach { case (params, impls) =>
			val p = new JavaPlot();
			if (!show) {
				val outfile = output / s"${b.symbol}-pairs${params._1}-pipe${params._2}-static${params._3}.eps";
				val epsf = new terminal.PostscriptTerminal(outfile.toString);
				epsf.setColor(true);
		        p.setTerminal(epsf);
			}
			p.getAxis("x").setLabel("#msgs/pair");
			p.getAxis("y").setLabel("throughput (msgs/s)");
			p.setTitle(s"${b.name} (#pairs = ${params._1}, #pipeline size = ${params._2}, static = ${params._3})");
			impls.foreach { case (key, res) =>
				val dsp = new gplot.DataSetPlot(res);
				dsp.setTitle(res.implLabel);
				val ps = new style.PlotStyle(style.Style.LINESPOINTS);
				val colour = colourMap(key);
				ps.setLineType(colour);
				ps.setPointType(pointMap(key));
				dsp.setPlotStyle(ps);
				p.addPlot(dsp);
			}
			p.plot();
		}
	}
	{
		val staticImpls = ImplResults.paramsToImpl(
			meanGroupedParams,
			(label: String, req: ThroughputPingPongRequest) => s"${label} (${if (req.staticOnly) { "Static" } else { "Alloc." }})"
		);
		val meanGrouped = ImplResults.slices(
			staticImpls,
			(req: ThroughputPingPongRequest) => (req.messagesPerPair, req.parallelism),
			(req: ThroughputPingPongRequest) => req.pipelineSize
		);
		meanGrouped.foreach { case (params, impls) =>
			val merged = ImplResults.merge(impls);
			//println("Merged:\n" + merged);
			val p = new JavaPlot();
			//GNUPlot.getDebugger().setLevel(gutils.Debug.VERBOSE);
			if (!show) {
				val outfile = output / s"${b.symbol}-msgs${params._1}-pairs${params._2}.eps";
				val epsf = new terminal.PostscriptTerminal(outfile.toString);
				epsf.setColor(true);
		        p.setTerminal(epsf);
			}
			p.getAxis("x").setLabel("pipeline size (#msgs)");
			p.getAxis("y").setLabel("throughput (msgs/s)");
			p.setTitle(s"${b.name} (#msgs/pair = ${params._1}, #pairs = ${params._2})");
			p.set("boxwidth", "0.8");
			p.set("style", "data histograms");
			merged.labels.zipWithIndex.foreach { case (label, i) =>
				//println(s"Setting $label at $i (column ${i + merged.dataColumnOffset})");
				val dsp = new gplot.DataSetPlot(merged);
				if (i == 0) {
					dsp.set("using", s"""${i + merged.dataColumnOffset + 1}:xtic(${1})""");
					// val ps = new style.PlotStyle(style.Style.HISTOGRAMS);
					// dsp.setPlotStyle(ps);
				} else {
					dsp.set("using", s"""${i + merged.dataColumnOffset + 1}""");
				}
				dsp.setTitle(label);
				//val ps = new style.PlotStyle();
				val ps = new style.PlotStyle(style.Style.HISTOGRAMS);
				val colour = colourMap2.find {
					case (k, v) => label.toLowerCase().startsWith(k.toLowerCase())
				}.map(_._2).getOrElse(style.NamedPlotColor.BLACK);
				ps.setLineType(colour);
				// // ps.setPointType(pointMap(key));
				dsp.setPlotStyle(ps);
				p.addPlot(dsp);
				val definitionSB = new java.lang.StringBuilder();
				dsp.retrieveDefinition(definitionSB);
				val definition = definitionSB.toString();
				println(s"Definition: $definition");
			}
			p.plot();
		}
	}
}

// From here: http://javaplot.panayotis.com/doc/com/panayotis/gnuplot/style/NamedPlotColor.html
private val colourMap: Map[String, style.PlotColor] = Map(
	"AKKA" -> style.NamedPlotColor.SKYBLUE,
	"KOMPACTAC" -> style.NamedPlotColor.SEA_GREEN,
	"KOMPACTCO" -> style.NamedPlotColor.SPRING_GREEN,
	"KOMPACTMIX" -> style.NamedPlotColor.FOREST_GREEN,
	"KOMPICSJ" -> style.NamedPlotColor.RED,
	"KOMPICSSC" -> style.NamedPlotColor.DARK_RED,
	"ACTIX" -> style.NamedPlotColor.DARK_BLUE
);

private val colourMap2: Map[String, style.PlotColor] = Map(
	"Akka" -> style.NamedPlotColor.SKYBLUE,
	"Kompact Actor" -> style.NamedPlotColor.SEA_GREEN,
	"Kompact Component" -> style.NamedPlotColor.SPRING_GREEN,
	"Kompact Mixed" -> style.NamedPlotColor.TURQUOISE ,
	"Kompics Java" -> style.NamedPlotColor.RED,
	"Kompics Scala" -> style.NamedPlotColor.DARK_RED,
	"Actix" -> style.NamedPlotColor.DARK_BLUE
);

private val pointMap: Map[String, Int] = 
	List("AKKA", "KOMPACTAC", "KOMPACTCO", "KOMPACTMIX", "KOMPICSJ", "KOMPICSSC", "ACTIX").zipWithIndex.toMap.mapValues(_ + 1);
