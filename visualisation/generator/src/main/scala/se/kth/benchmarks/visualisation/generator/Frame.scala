package se.kth.benchmarks.visualisation.generator

import scalatags.Text.all._
import scalatags.generic.Attr

object Frame {
  val integrity: Attr = Attr("integrity");
  val crossorigin: Attr = Attr("crossorigin");

  val headers = head(
    scalatags.Text.tags2.title("MPP Suite Index"),
    link(rel := "stylesheet", `type` := "text/css", href := "bootstrap.min.css"),
    link(rel := "stylesheet", `type` := "text/css", href := "main.css"),
    link(rel := "stylesheet", `type` := "text/css", href := "standard.css"),
    script(src := "https://code.jquery.com/jquery-1.12.4.min.js",
           integrity := "sha256-ZosEbRLbNQzLpnKIkEdrPv7lOy9C27hHQ+Xp8a4MxAQ=",
           crossorigin := "anonymous"),
    script(src := "https://code.highcharts.com/highcharts.js", crossorigin := "anonymous"),
    script(src := "https://code.highcharts.com/modules/data.js", crossorigin := "anonymous"),
    script(src := "benchmark-suite-plotting.js")
  );

  def embed(content: Tag): String =
    "<!DOCTYPE html>" + html(
      headers,
      content
    ).render;
}
