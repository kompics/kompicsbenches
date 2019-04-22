#!/usr/bin/env amm

import ammonite.ops._
import java.lang.{Process, ProcessBuilder}
import scala.collection.JavaConverters._
import $file.build, build.{relps, relp, binp, format}

type AddressArg = String;
type LocalRunner = (AddressArg) => Runner;
type RemoteRunner = (AddressArg, AddressArg, Int) => Runner;
type ClientRunner = (AddressArg, AddressArg) => Runner;

case class BenchmarkImpl(symbol: String, label: String, local: LocalRunner, remote: RemoteRunner, client: ClientRunner) {
	def localRunner(benchRunnerAddr: AddressArg): BenchmarkRunner = 
		BenchmarkRunner(info, local(benchRunnerAddr));
	def remoteRunner(benchRunnerAddr: AddressArg, benchMasterAddr: AddressArg, numClients: Int): BenchmarkRunner = 
		BenchmarkRunner(info, remote(benchRunnerAddr, benchMasterAddr, numClients));
	def clientRunner(benchMasterAddr: AddressArg, benchClientAddr: AddressArg): BenchmarkRunner = 
		BenchmarkRunner(info, client(benchMasterAddr, benchClientAddr));
	def info: BenchmarkInfo = BenchmarkInfo(symbol, label);
}

case class BenchmarkInfo(symbol: String, label: String)

case class Runner(env: Path, exec: Path, args: Seq[Shellable])

case class BenchmarkRunner(bench: BenchmarkInfo, runner: Runner) {
	def symbol: String = bench.symbol;
	def label: String = bench.label;
	def run(logFolder: Path): Process = {
		val command = (runner.exec.toString +: runner.args.flatMap(_.s)).toList.asJava;
		val pb = new ProcessBuilder(command);
		val env = pb.environment();
		env.put("RUST_BACKTRACE", "1"); // TODO remove this for non-testing!
		pb.directory(runner.env.toIO);
		pb.redirectError(ProcessBuilder.Redirect.appendTo(errorLog(logFolder)));
		pb.redirectOutput(ProcessBuilder.Redirect.appendTo(outputLog(logFolder)));
		val childProcess = pb.start();
		val closeChildThread = new Thread() {
		    override def run(): Unit = {
		        childProcess.destroy();
		    }
		};
		Runtime.getRuntime().addShutdownHook(closeChildThread); 
		childProcess
	}
	lazy val fileLabel: String = bench.label.toLowerCase().replaceAll(" ", "_");
	def outputLog(logFolder: Path) = (logFolder / s"${fileLabel}.out").toIO;
	def errorLog(logFolder: Path) = (logFolder / s"${fileLabel}.error").toIO;
}

val javaBin = binp('java);

val implementations: Map[String, BenchmarkImpl] = Map(
	"AKKA" -> BenchmarkImpl(
		symbol="AKKA", 
		label="Akka", 
		local = (benchRunnerAddr) => Runner(relp("akka"), javaBin, Seq("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("akka"), javaBin, Seq("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("akka"), javaBin, Seq("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchMasterAddr, benchClientAddr))
	),
	"KOMPSC" -> BenchmarkImpl(
		symbol="KOMPSC", 
		label="Kompics Scala", 
		local = (benchRunnerAddr) => Runner(relp("kompics_scala"), javaBin, Seq("-jar", "target/scala-2.12/Kompics Benchmark Suite-assembly-0.1.0-SNAPSHOT.jar", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompics_scala"), javaBin, Seq("-jar", "target/scala-2.12/Kompics Benchmark Suite-assembly-0.1.0-SNAPSHOT.jar", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompics_scala"), javaBin, Seq("-jar", "target/scala-2.12/Kompics Benchmark Suite-assembly-0.1.0-SNAPSHOT.jar", benchMasterAddr, benchClientAddr))
	),
	"KOMPACTAC" -> BenchmarkImpl(
		symbol="KOMPACTAC", 
		label="Kompact Actor", 
		local = (benchRunnerAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("actor", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("actor", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("actor", benchMasterAddr, benchClientAddr))
	),
	"KOMPACTCO" -> BenchmarkImpl(
		symbol="KOMPACTCO", 
		label="Kompact Component", 
		local = (benchRunnerAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("component", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("component", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("component", benchMasterAddr, benchClientAddr))
	),
	"KOMPACTMIX" -> BenchmarkImpl(
		symbol="KOMPACTMIX", 
		label="Kompact Mixed", 
		local = (benchRunnerAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("mixed", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("mixed", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("mixed", benchMasterAddr, benchClientAddr))
	),
);
