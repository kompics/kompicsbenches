package se.kth.benchmarks.runner

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.Future
import scala.util.{ Try, Success, Failure }
import com.lkroll.common.macros.Macros

case class BenchmarkRun[Params](
  name:   String,
  symbol: String,
  invoke: (Runner.Stub, Params) => Future[TestResult]);

trait ParameterDescription {
  def toCSV: String;
  def toSuffix: String;
}

trait ParameterDescriptor[P] {
  def toCSV(p: P): String;
  def toSuffix(p: P): String;
}
trait ParameterDescriptionImplicits {
  class NumDescr[N: Numeric] extends ParameterDescriptor[N] {
    override def toCSV(p: N): String = s"$p";
    override def toSuffix(p: N): String = s"$p";
  }
  implicit val intDescr: ParameterDescriptor[Int] = new NumDescr[Int];
  implicit val longDescr: ParameterDescriptor[Long] = new NumDescr[Long];

  implicit class ParameterDescriptorDescription[P: ParameterDescriptor](p: P) extends ParameterDescription {
    private def desc = implicitly[ParameterDescriptor[P]];

    override def toCSV: String = desc.toCSV(p);
    override def toSuffix: String = desc.toSuffix(p);
  }
}
object ParameterDescriptionImplicits extends ParameterDescriptionImplicits;

trait ParameterSpace[Params] {
  def foreach(f: Params => Unit);
  def describe(p: Params): ParameterDescription;
}
case class ParametersSparse1D[T: ParameterDescriptor](params: Seq[T]) extends ParameterSpace[T] {
  import ParameterDescriptionImplicits.ParameterDescriptorDescription;

  override def foreach(f: T => Unit) = params.foreach(f);
  override def describe(p: T): ParameterDescription = p;
}

trait Benchmark {
  def name: String;
  def symbol: String;
  def withStub(stub: Runner.Stub)(f: (Future[TestResult], ParameterDescription) => Unit): Unit;
}
object Benchmark {
  def apply[Params](b: BenchmarkRun[Params], space: ParameterSpace[Params]): Benchmark = BenchmarkWithSpace(b, space);
  def apply[Params](
    name:   String,
    symbol: String,
    invoke: (Runner.Stub, Params) => Future[TestResult],
    space:  ParameterSpace[Params]): Benchmark = BenchmarkWithSpace(BenchmarkRun(name, symbol, invoke), space);
}
case class BenchmarkWithSpace[Params](b: BenchmarkRun[Params], space: ParameterSpace[Params]) extends Benchmark {
  override def name: String = b.name;
  override def symbol: String = b.symbol;
  def run = b.invoke;
  override def withStub(stub: Runner.Stub)(f: (Future[TestResult], ParameterDescription) => Unit): Unit = {
    space.foreach(p => f(run(stub, p), space.describe(p)))
  }
}

object Benchmarks extends ParameterDescriptionImplicits {

  implicit class ExtLong(i: Long) {
    def mio: Long = i * 1000000l;
    def k: Long = i * 1000l;
  }

  implicit def seq2param[T: ParameterDescriptor](s: Seq[T]): ParameterSpace[T] = ParametersSparse1D(s);

  val pingpong = Benchmark(
    name = "Ping Pong",
    symbol = "PINGPONG",
    invoke = (stub, n: Long) => {
      val request = PingPongRequest(numberOfMessages = n);
      stub.pingPong(request)
    },
    //space = 1l.mio to 10l.mio by 1l.mio);
    space = 10l.k to 100l.k by 10l.k);

  val netpingpong = Benchmark(
    name = "Net Ping Pong",
    symbol = "NETPINGPONG",
    invoke = (stub, n: Long) => {
      val request = PingPongRequest(numberOfMessages = n);
      stub.pingPong(request)
    },
    //space = 1l.mio to 10l.mio by 1l.mio);
    space = 1l.k to 10l.k by 1l.k);

  val benchmarks: List[Benchmark] = Macros.memberList[Benchmark];
}
