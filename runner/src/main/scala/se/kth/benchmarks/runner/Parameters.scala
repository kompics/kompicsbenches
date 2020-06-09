package se.kth.benchmarks.runner

//import com.google.protobuf.{ Message, TextFormat }
import scalapb.{GeneratedMessageCompanion => Companion}
import scala.reflect.runtime.{currentMirror => cm}
import scala.reflect.runtime.universe._

object Parameters {
  type Message[A] = scalapb.GeneratedMessage with scalapb.Message[A];
}
import Parameters._;

trait ParameterDescription {
  def toCSV: String;
  def toPath: String;
}

trait ParameterDescriptor[P] {
  def toCSV(p: P): String;
  def toPath(p: P): String;
  def fromCSV(s: String): P;
}

class ProtobufDescriptor[P <: Message[P]](implicit tag: TypeTag[P]) extends ParameterDescriptor[P] {

  lazy val companion: Companion[P] = {
    val companionModule = tag.tpe.typeSymbol.companion.asModule
    cm.reflectModule(companionModule).instance.asInstanceOf[Companion[P]]
  }

  override def toCSV(p: P): String = {
    val s = p.toProtoString;
    //TextFormat.printToUnicodeString(p)
    // remove things that aren't allowed in CSV files
    s.replaceAll(",", "!C!").replaceAll("""\R""", "!N!").replaceAll("\"", "\"\"")
  }
  override def toPath(p: P): String = {
    //TextFormat.printToString(p)
    val s = p.toProtoString;
    // remove things that aren't allowed in file names
    s.replaceAll("[^a-zA-Z0-9-_\\.]", "_")
  }
  override def fromCSV(s: String): P = {
    val fixed = ProtobufDescriptor.unescapeCSV(s);
    companion.fromAscii(fixed)
  }
}
object ProtobufDescriptor {
  def unescapeCSV(s: String): String = {
    s.replaceAll("\"\"", "\"").replaceAll("!N!", "\n").replaceAll("!C!", ",")
  }
}

trait ParameterDescriptionImplicits {
  //  class NumDescr[N: Numeric] extends ParameterDescriptor[N] {
  //    override def toCSV(p: N): String = s"$p";
  //    override def toSuffix(p: N): String = s"$p";
  //  }
  //  implicit val intDescr: ParameterDescriptor[Int] = new NumDescr[Int];
  //  implicit val longDescr: ParameterDescriptor[Long] = new NumDescr[Long];

  implicit class ParameterDescriptorDescription[P: ParameterDescriptor](p: P) extends ParameterDescription {
    private def desc = implicitly[ParameterDescriptor[P]];

    override def toCSV: String = desc.toCSV(p);
    override def toPath: String = desc.toPath(p);
  }

  def protobufDescr[P <: Message[P]: TypeTag]: ParameterDescriptor[P] = new ProtobufDescriptor[P]();
}
object ParameterDescriptionImplicits extends ParameterDescriptionImplicits;

trait ParameterSpace[Params] {
  def foreach(f: Params => Unit);
  def size: Long;
  def describe(p: Params): ParameterDescription;
  def paramsFromCSV(s: String): Params;
}

case class ParametersSparse1D[T](params: Seq[T])(implicit private val descr: ParameterDescriptor[T])
    extends ParameterSpace[T] {
  import ParameterDescriptionImplicits.ParameterDescriptorDescription;

  override def foreach(f: T => Unit) = params.foreach(f);
  override def size: Long = params.size;
  override def describe(p: T): ParameterDescription = p;
  override def paramsFromCSV(s: String): T = descr.fromCSV(s);
}

class ParameterSpacePB[P <: Message[P]: TypeTag](val params: Seq[P]) extends ParameterSpace[P] {
  import ParameterDescriptionImplicits._;

  implicit private val descr = protobufDescr[P];

  override def foreach(f: P => Unit) = params.foreach(f);
  override def size: Long = params.size;
  override def describe(p: P): ParameterDescription = p;
  override def paramsFromCSV(s: String): P = descr.fromCSV(s);
}
object ParameterSpacePB {
  def apply[P <: Message[P]: TypeTag](params: Seq[P]): ParameterSpacePB[P] = new ParameterSpacePB[P](params);
  def mapped[T](p: Traversable[T]): TupleSpace[T] = new TupleSpace(p);
  def cross[T1, T2](p1: Traversable[T1], p2: Traversable[T2]): TupleSpace[(T1, T2)] = {
    val iter = for {
      i1 <- p1;
      i2 <- p2
    } yield (i1, i2);
    new TupleSpace(iter)
  }
  def cross[T1, T2, T3](p1: Traversable[T1], p2: Traversable[T2], p3: Traversable[T3]): TupleSpace[(T1, T2, T3)] = {
    val iter = for {
      i1 <- p1;
      i2 <- p2;
      i3 <- p3
    } yield (i1, i2, i3);
    new TupleSpace(iter)
  }
  def cross[T1, T2, T3, T4](p1: Traversable[T1],
                            p2: Traversable[T2],
                            p3: Traversable[T3],
                            p4: Traversable[T4]): TupleSpace[(T1, T2, T3, T4)] = {
    val iter = for {
      i1 <- p1;
      i2 <- p2;
      i3 <- p3;
      i4 <- p4
    } yield (i1, i2, i3, i4);
    new TupleSpace(iter)
  }
  def cross[T1, T2, T3, T4, T5](p1: Traversable[T1],
                                p2: Traversable[T2],
                                p3: Traversable[T3],
                                p4: Traversable[T4],
                                p5: Traversable[T5]): TupleSpace[(T1, T2, T3, T4, T5)] = {
    val iter = for {
      i1 <- p1;
      i2 <- p2;
      i3 <- p3;
      i4 <- p4;
      i5 <- p5
    } yield (i1, i2, i3, i4, i5);
    new TupleSpace(iter)
  }
  def cross[T1, T2, T3, T4, T5, T6](p1: Traversable[T1],
                                p2: Traversable[T2],
                                p3: Traversable[T3],
                                p4: Traversable[T4],
                                p5: Traversable[T5],
                                p6: Traversable[T6]): TupleSpace[(T1, T2, T3, T4, T5, T6)] = {
    val iter = for {
      i1 <- p1;
      i2 <- p2;
      i3 <- p3;
      i4 <- p4;
      i5 <- p5;
      i6 <- p6
    } yield (i1, i2, i3, i4, i5, i6);
    new TupleSpace(iter)
  }

  class TupleSpace[T](val iter: Traversable[T]) {
    def msg[M <: Message[M]: TypeTag](f: T => M): ParameterSpacePB[M] = {
      ParameterSpacePB(iter.map(f).toSeq)
    }

    def append(other: TupleSpace[T]): TupleSpace[T] = {
      val iter = this.iter ++ other.iter;
      new TupleSpace(iter)
    }
  }
}
