#!/usr/bin/env amm

import ammonite.ops._
import ammonite.ops.ImplicitWd._
//import $ivy.`com.lihaoyi::scalatags:0.6.2`, scalatags.Text.all._
//import $ivy.`org.sameersingh::scalaplot:0.0.4`, org.sameersingh.scalaplot.Implicits._
import $ivy.`com.panayotis.javaplot:javaplot:0.5.0`, com.panayotis.gnuplot.{plot => gplot, _}
import $ivy.`com.github.tototoshi::scala-csv:1.3.5`, com.github.tototoshi.csv._
import $file.build
import java.io.File
import $file.benchmarks, benchmarks.{BenchmarkImpl, implementations}
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
	val mean = rawData.map(m => (m("IMPL"), m("PARAMS").toLong, m("MEAN").toDouble));
	val meanGrouped = mean.groupBy(_._1).map { case (key, entries) =>
		val params = entries.map(_._2);
		val means = entries.map(_._3);
		(key -> Impl2DResult(implementations(key), params, means))
	};
	println(meanGrouped);
	val fileName = data.last.toString.replace(".data", "");
	val p = new JavaPlot();
	if (!show) {
		val outfile = output / s"${fileName}.eps";
		val epsf = new terminal.PostscriptTerminal(outfile.toString);
		epsf.setColor(true);
        p.setTerminal(epsf);
	}
	p.getAxis("x").setLabel("#msgs");
	p.getAxis("y").setLabel("execution time (ms)");
	p.setTitle(fileName);
	meanGrouped.foreach { case (key, res) =>
		val dsp = new gplot.DataSetPlot(res);
		dsp.setTitle(res.impl.label);
		val ps = new style.PlotStyle(style.Style.LINESPOINTS);
		val colour = colourMap(key);
		ps.setLineType(colour);
		ps.setPointType(pointMap(key));
		dsp.setPlotStyle(ps);
		p.addPlot(dsp);
	}
	p.plot();
}

case class Impl2DResult(impl: BenchmarkImpl, params: List[Long], means: List[Double]) extends dataset.DataSet {
	override def getDimensions(): Int = 2;
	override def getPointValue(point: Int, dimension: Int): String = dimension match {
		case 0 => params(point).toString
		case 1 => means(point).toString
		case _ => ???
	};
	override def size(): Int = means.size;
}

private val colourMap: Map[String, style.PlotColor] = Map(
	"AKKA" -> style.NamedPlotColor.SKYBLUE,
	"KOMPACTAC" -> style.NamedPlotColor.SEA_GREEN,
	"KOMPACTCO" -> style.NamedPlotColor.SPRING_GREEN,
	"KOMPACTMIX" -> style.NamedPlotColor.FOREST_GREEN,
	"KOMPICSJ" -> style.NamedPlotColor.RED,
	"KOMPICSSC" -> style.NamedPlotColor.DARK_RED
);

private val pointMap: Map[String, Int] = 
	List("AKKA", "KOMPACTAC", "KOMPACTCO", "KOMPACTMIX", "KOMPICSJ", "KOMPICSSC").zipWithIndex.toMap.mapValues(_ + 1);
