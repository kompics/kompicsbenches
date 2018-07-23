#!/usr/bin/env amm

import ammonite.ops._
import ammonite.ops.ImplicitWd._
//import $ivy.`com.lihaoyi::scalatags:0.6.2`, scalatags.Text.all._
//import $ivy.`org.sameersingh::scalaplot:0.0.4`, org.sameersingh.scalaplot.Implicits._
import $file.build
import build.{relps, relp, binp}

val results = pwd / 'results;
val plots = pwd / 'plots;

@main
def main(): Unit = {
	if (!(exists! results)) {
		println("No results to plot.");
		return;
	}
	if (exists! plots) {
		rmdir! plots;
	}
	mkdir! plots;
	(ls! results).foreach { run =>
		println(run);
	}
	// yeah, nvm for now
}