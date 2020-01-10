package se.kth.benchmarks.visualisation.generator

import scalatags.Text.all._
import scalatags.Text.tags2.nav
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
    script(src := "https://code.highcharts.com/highcharts-more.js", crossorigin := "anonymous"),
    script(src := "https://code.highcharts.com/modules/exporting.js", crossorigin := "anonymous"),
    script(src := "https://code.highcharts.com/modules/export-data.js", crossorigin := "anonymous"),
    script(src := "https://code.highcharts.com/modules/data.js", crossorigin := "anonymous"),
    script(src := "benchmark-suite-plotting.js")
  );

  val ariaCurrent = attr("aria-current");

  def embed(content: Tag, title: String): String =
    "<!DOCTYPE html>" + html(
      headers,
      body(
        div(
          BootstrapStyle.containerFluid,
          h1(BootstrapStyle.textCentre, "MPP Suite Experiment Run"),
          title match {
            case IndexPage.title =>
              nav(aria.label := "breadcrumb",
                  ol(BootstrapStyle.breadcrumb,
                     li(BootstrapStyle.active, BootstrapStyle.breadcrumbItem, ariaCurrent := "page", title)))
            case _ =>
              nav(
                aria.label := "breadcrumb",
                ol(
                  BootstrapStyle.breadcrumb,
                  li(BootstrapStyle.breadcrumbItem, a(href := "index.html", IndexPage.title)),
                  li(BootstrapStyle.active, BootstrapStyle.breadcrumbItem, ariaCurrent := "page", title)
                )
              )
          }
        ),
        content
      )
    ).render;

  val backToTopButton = div(
    BootstrapStyle.floatRight,
    button(`type` := "button",
           BootstrapStyle.btn,
           BootstrapStyle.btnSecondary,
           BootstrapStyle.btnSmall,
           "back to top",
           onclick := "Plotting.backToTop();")
  );
}
