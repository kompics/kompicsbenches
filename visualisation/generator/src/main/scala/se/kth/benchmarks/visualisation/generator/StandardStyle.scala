package se.kth.benchmarks.visualisation.generator

import scalatags.Text.all._
import scalatags.stylesheet._

object StandardStyle extends StyleSheet {
  override def customSheetName = Some("mpp");

  initStyleSheet()

  val headline = cls(
    textDecoration := "underline"
  );

  val hidden = cls(
    display := "none"
  );

  val navLink = cls(
    padding := ".5rem 1rem"
  );

  val navBox = cls(
    alignItems := "flex-start!important"
  );
  val navBoxInner = cls(
    backgroundColor := "#e9ecef",
    padding := 5.px,
    borderRadius := 6.px
  );
}
