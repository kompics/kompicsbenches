package se.kth.benchmarks.visualisation.plotting

import scala.scalajs.js.annotation._
import scalajs.js, js.UndefOr
import org.scalajs.dom
import org.scalajs.jquery._
import com.highcharts.HighchartsUtils._
import com.highcharts.HighchartsAliases._
import com.highcharts.config._
import scalatags.JsDom.all._
import org.scalajs.dom.raw.Event
import com.highcharts.CleanJsObject

@JSExportTopLevel("Plotting")
object Plotting {
  scribe.info("Plotting loaded!");

  @JSExport
  def backToTop(): Unit = {
    jQuery("html, body").animate(js.Dictionary("scrollTop" -> 0), "50");
  }

  @JSExport
  def plotSeries(plotTitle: String,
                 xAxisLabel: String,
                 xAxisCategories: js.Array[String],
                 yAxisLabel: String,
                 seriesData: js.Array[AnySeries],
                 target: dom.html.Element): Unit = {
    jQuery(target).highcharts(new HighchartsConfig {
      override val chart: Cfg[Chart] = Chart(`type` = "line", zoomType = "x");
      override val title: Cfg[Title] = Title(text = plotTitle);
      override val xAxis: CfgArray[XAxis] =
        js.Array(XAxis(title = XAxisTitle(text = xAxisLabel), categories = xAxisCategories, minRange = 2));
      override val yAxis: CfgArray[YAxis] = js.Array(YAxis(title = YAxisTitle(text = yAxisLabel), `type` = "linear"));
      override val series: SeriesCfg = seriesData;
    });
  }
}
