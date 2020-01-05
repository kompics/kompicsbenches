package se.kth.benchmarks.visualisation.generator

import scalatags.Text.all._

object IndexPage {
  def generate(): String = "<!DOCTYPE html>" + page.render;

  lazy val page = html(
    head(
      scalatags.Text.tags2.title("MPP Suite Index"),
      link(rel := "stylesheet", `type` := "text/css", href := "standard.css"),
      script(src := "benchmark-suite-plotting.js"),
      script("var p = Plotting;")
    ),
    body(
      h1(StandardStyle.headline, "Headline")
    )
  );
}
