package se.kth.benchmarks.visualisation.generator

import scalatags.Text.all._

object IndexPage {

  val title = "Experiments";
}
case class IndexPage(plots: List[Plot]) {
  import IndexPage._;

  def generate(): String = Frame.embed(page, title);

  lazy val page =
    div(
      BootstrapStyle.container,
      h2(title),
      div(
        BootstrapStyle.listGroup,
        for (plot <- plots.sortBy(_.title))
          yield a(href := plot.relativePath,
                  plot.title,
                  BootstrapStyle.listGroupItem,
                  BootstrapStyle.listGroupItemAction)
      )
    );
}
