package se.kth.benchmarks.visualisation.generator

object FrameworkPlotStyle {
  val Colors:Map[String, String] = Map(
    "'Akka'"-> "'Blue'",
    "'Akka Typed'" -> "'MediumTurquoise'",
    "'Erlang'" -> "'Red'",
    "'Kompact Actor'" -> "'Black'",
    "'Kompact Component'" -> "'DarkGrey'",
    "'Kompact Mixed'" -> "'Orange'",
    "'Kompics Java'" -> "'Green'",
    "'Kompics Scala 1.x'" -> "'Violet'",
    "'Kompics Scala 2.x'" -> "'Purple'",
    "'Riker'" -> "'Grey'",
    "'Actix'" -> "'Lime'",
  );

  val Markers:Map[String, String] = Map(
    "'Akka'"-> "symbol: 'circle', radius: 6",
    "'Akka Typed'" -> "symbol: 'triangle', radius: 6",
    "'Erlang'" -> "symbol: 'square', radius: 6",
    "'Kompact Actor'" -> "symbol: 'diamond', radius: 6",
    "'Kompact Component'" -> "symbol: 'triangle-down', radius: 6",
    "'Kompact Mixed'" -> "symbol: 'circle'",
    "'Kompics Java'" -> "symbol: 'square'",
    "'Kompics Scala 1.x'" -> "symbol: 'diamond'",
    "'Kompics Scala 2.x'" -> "symbol: 'triangle-down'",
    "'Riker'" -> "symbol: 'triangle'",
    "'Actix'" -> "symbol: 'circle'",
  );

  val DashStyles:Map[String, String] = Map(
    "'Akka'"-> "'Solid'",
    "'Akka Typed'" -> "'ShortDash'",
    "'Erlang'" -> "'Solid'",
    "'Kompact Actor'" -> "'Solid'",
    "'Kompact Component'" -> "'ShortDash'",
    "'Kompact Mixed'" -> "'ShortDot'",
    "'Kompics Java'" -> "'Solid'",
    "'Kompics Scala 1.x'" -> "'ShortDash'",
    "'Kompics Scala 2.x'" -> "'ShortDot'",
    "'Riker'" -> "'Solid'",
    "'Actix'" -> "'ShortDash'",
  );


  def getColor(framework: String): String = {
    val default_color = "'Yellow'"
    if (framework.contains(" error") ) {
      val f = framework.substring(0, framework.indexOf(" error"));
      Colors.getOrElse(f+"'", default_color)
    } else {
      Colors.getOrElse(framework, default_color)
    }
  }

  def getMarker(framework: String): String = {
    val default_symbol = "diamond"
    if (framework.contains(" error") ) {
      val f = framework.substring(0, framework.indexOf(" error"));
      Markers.getOrElse(f+"'", default_symbol)
    } else {
      Markers.getOrElse(framework, default_symbol)
    }
  }

  def getDashStyle(framework: String): String = {
    val default_style = "'Solid'"
    if (framework.contains(" error") ) {
      val f = framework.substring(0, framework.indexOf(" error"));
      DashStyles.getOrElse(f+"'", default_style)
    } else {
      DashStyles.getOrElse(framework, default_style)
    }
  }
}
