package se.kth.benchmarks.visualisation.generator

import com.typesafe.scalalogging.StrictLogging
import java.io.File
import scala.util.{Failure, Success, Try}

object Plotter extends StrictLogging {
  def fromSource(source: File): Try[Plotted] = Try {
    logger.debug(s"Plotting $source");
    val sourceName = source.getName();
    val title = sourceName.substring(0, sourceName.length() - 5);
    val fileName = s"${title}.html";
    Plotted(title, fileName, "test")
  };
}
case class Plot(title: String, relativePath: String)
case class Plotted(title: String, fileName: String, text: String)
