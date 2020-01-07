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
  scribe.info("Loaded!");

  @JSExport
  def plot(plotTitle: String, units: String, dataSrc: dom.html.Element, target: dom.html.Element): Unit = {
    jQuery(target).highcharts(new HighchartsConfig {
      override val chart: Cfg[Chart] = Chart(`type` = "column");
      override val title: Cfg[Title] = Title(text = plotTitle);
      override val yAxis: CfgArray[YAxis] = js.Array(YAxis(title = YAxisTitle(text = units)));
      override val data: UndefOr[CleanJsObject[Data]] = Data(csv = dataSrc.innerHTML);
    });
  }
  @JSExport
  def plotSeries(plotTitle: String,
                 xAxisLabel: String,
                 xAxisCategories: js.Array[String],
                 yAxisLabel: String,
                 seriesData: js.Array[AnySeries],
                 target: dom.html.Element): Unit = {
    jQuery(target).highcharts(new HighchartsConfig {
      override val chart: Cfg[Chart] = Chart(`type` = "line");
      override val title: Cfg[Title] = Title(text = plotTitle);
      override val xAxis: CfgArray[XAxis] =
        js.Array(XAxis(title = XAxisTitle(text = xAxisLabel), categories = xAxisCategories));
      override val yAxis: CfgArray[YAxis] = js.Array(YAxis(title = YAxisTitle(text = yAxisLabel)));
      override val series: SeriesCfg = seriesData;
    });
  }
  @JSExport
  def test(): Unit = {
    val container = div("Test").render;
    dom.document.body.appendChild(container);
    jQuery("#container").highcharts(new HighchartsConfig {
      // Chart config
      override val chart: Cfg[Chart] = Chart(`type` = "bar")

      // Chart title
      override val title: Cfg[Title] = Title(text = "Demo bar chart")

      // X Axis settings
      override val xAxis: CfgArray[XAxis] = js.Array(XAxis(categories = js.Array("Apples", "Bananas", "Oranges")))

      // Y Axis settings
      override val yAxis: CfgArray[YAxis] = js.Array(YAxis(title = YAxisTitle(text = "Fruit eaten")))

      // Series
      override val series: SeriesCfg = js.Array[AnySeries](
        SeriesBar(name = "Jane", data = js.Array[Double](1, 0, 4)),
        SeriesBar(name = "John", data = js.Array[Double](5, 7, 3))
      )
    })
  }
}
