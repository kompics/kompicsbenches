package se.kth.benchmarks.visualisation.generator

import scalatags.Text.all._
import scalatags.stylesheet._

object StandardStyle extends StyleSheet {
  override def customSheetName = Some("mpp");

  initStyleSheet()

  val headline = cls(
    textDecoration := "underline"
  );

  val hidden = cls {
    display := "none"
  }
}
