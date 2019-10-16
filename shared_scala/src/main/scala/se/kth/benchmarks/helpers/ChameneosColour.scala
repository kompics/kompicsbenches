package se.kth.benchmarks.helpers

sealed trait ChameneosColour {
  def complement(other: ChameneosColour): ChameneosColour;
};
object ChameneosColour {

  def forId(i: Int): ChameneosColour = {
    (i % 3) match {
      case 0 => Red
      case 1 => Yellow
      case 2 => Blue
    }
  }

  case object Red extends ChameneosColour {
    override def complement(other: ChameneosColour): ChameneosColour = other match {
      case Red    => Red
      case Yellow => Blue
      case Blue   => Yellow
      case Faded  => Faded
    }
  }
  case object Yellow extends ChameneosColour {
    override def complement(other: ChameneosColour): ChameneosColour = other match {
      case Red    => Blue
      case Yellow => Yellow
      case Blue   => Red
      case Faded  => Faded
    }
  }
  case object Blue extends ChameneosColour {
    override def complement(other: ChameneosColour): ChameneosColour = other match {
      case Red    => Yellow
      case Yellow => Red
      case Blue   => Blue
      case Faded  => Faded
    }
  }
  case object Faded extends ChameneosColour {
    override def complement(other: ChameneosColour): ChameneosColour = Faded;
  }
}
