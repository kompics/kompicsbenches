package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.Future
import scala.util.{ Try, Success, Failure }
import com.lkroll.common.macros.Macros

case class BenchmarkRun[Params](
  name:   String,
  symbol: String,
  invoke: (Runner.Stub, Params) => Future[TestResultMessage]);

trait ParameterSpace[Params] {
  def foreach(f: Params => Unit);
}
case class ParametersSparse1D[T](params: Seq[T]) extends ParameterSpace[T] {
  override def foreach(f: T => Unit) = params.foreach(f);
}

trait Benchmark {
  def name: String;
  def symbol: String;
  def withStub(stub: Runner.Stub)(f: Future[TestResultMessage] => Unit): Unit;
}
object Benchmark {
  def apply[Params](b: BenchmarkRun[Params], space: ParameterSpace[Params]): Benchmark = BenchmarkWithSpace(b, space);
  def apply[Params](
    name:   String,
    symbol: String,
    invoke: (Runner.Stub, Params) => Future[TestResultMessage],
    space:  ParameterSpace[Params]): Benchmark = BenchmarkWithSpace(BenchmarkRun(name, symbol, invoke), space);
}
case class BenchmarkWithSpace[Params](b: BenchmarkRun[Params], space: ParameterSpace[Params]) extends Benchmark {
  override def name: String = b.name;
  override def symbol: String = b.symbol;
  def run = b.invoke;
  override def withStub(stub: Runner.Stub)(f: Future[TestResultMessage] => Unit): Unit = {
    space.foreach(p => f(run(stub, p)))
  }
}

object Benchmarks {

  implicit def seq2param[T](s: Seq[T]): ParameterSpace[T] = ParametersSparse1D(s);

  val pingpong = Benchmark(
    name = "Ping Pong",
    symbol = "PINGPONG",
    invoke = (stub, n: Int) => {
      val request = PingPongRequest(numberOfMessages = n * 1000000);
      stub.pingPong(request)
    },
    space = 1 to 10);

  val benchmarks: List[Benchmark] = Macros.memberList[Benchmark];
}
