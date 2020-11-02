#!/usr/bin/env amm

import ammonite.ops._
import java.lang.{Process, ProcessBuilder}
import scala.collection.JavaConverters._
import $file.build, build.{relps, relp, binp, format}

type AddressArg = String;
type LocalRunner = (AddressArg) => Runner;
type RemoteRunner = (AddressArg, AddressArg, Int) => Runner;
type ClientRunner = (AddressArg, AddressArg) => Runner;

case class BenchmarkImpl(
	symbol: String, 
	label: String, 
	local: LocalRunner, 
	remote: RemoteRunner, 
	client: ClientRunner,
	mustCopy: List[Path]) {
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
		//env.put("RUST_BACKTRACE", "1"); // TODO remove this for non-testing!
		//env.put("JAVA_OPTS", "-Xms1G -Xmx32G -XX:+UseG1GC");
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
//val javaOpts = Seq[Shellable]("-Xms1G", "-Xmx32G", "-XX:+UseG1GC","-XX:+HeapDumpOnOutOfMemoryError");
val javaOpts = Seq[Shellable]("-Xms1G", "-Xmx16G", "-XX:+UseG1GC","-XX:+HeapDumpOnOutOfMemoryError");

val implementations: Map[String, BenchmarkImpl] = Map(
	"AKKA" -> BenchmarkImpl(
		symbol="AKKA",
		label="Akka",
		local = (benchRunnerAddr) => Runner(relp("akka"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.3.1-SNAPSHOT.jar", "untyped", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("akka"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.3.1-SNAPSHOT.jar", "untyped", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("akka"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.3.1-SNAPSHOT.jar", "untyped", benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("akka/target/scala-2.12/Akka Benchmark Suite-assembly-0.3.1-SNAPSHOT.jar"))
	),
	"AKKATYPED" -> BenchmarkImpl(
		symbol="AKKATYPED",
		label="Akka Typed",
		local = (benchRunnerAddr) => Runner(relp("akka"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.3.1-SNAPSHOT.jar", "typed", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("akka"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.3.1-SNAPSHOT.jar", "typed", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("akka"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.3.1-SNAPSHOT.jar", "typed", benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("akka/target/scala-2.12/Akka Benchmark Suite-assembly-0.3.1-SNAPSHOT.jar"))
	),
	"KOMPICSSC" -> BenchmarkImpl(
		symbol="KOMPICSSC",
		label="Kompics Scala 1.x",
		local = (benchRunnerAddr) => Runner(relp("kompics"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "kompicsscala1x/target/scala-2.12/Kompics Scala 1.x Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompics"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "kompicsscala1x/target/scala-2.12/Kompics Scala 1.x Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompics"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "-jar", "kompicsscala1x/target/scala-2.12/Kompics Scala 1.x Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("kompics/kompicsscala1x/target/scala-2.12/Kompics Scala 1.x Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar"))
	),
	"KOMPICSSC2" -> BenchmarkImpl(
		symbol="KOMPICSSC2",
		label="Kompics Scala 2.x",
		local = (benchRunnerAddr) => Runner(relp("kompics"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "kompicsscala2x/target/scala-2.12/Kompics Scala 2.x Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompics"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "kompicsscala2x/target/scala-2.12/Kompics Scala 2.x Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompics"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "-jar", "kompicsscala2x/target/scala-2.12/Kompics Scala 2.x Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("kompics/kompicsscala2x/target/scala-2.12/Kompics Scala 2.x Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar"))
	),
	"KOMPICSJ" -> BenchmarkImpl(
		symbol="KOMPICSJ",
		label="Kompics Java",
		local = (benchRunnerAddr) => Runner(relp("kompics"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "kompicsjava/target/scala-2.12/Kompics Java Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompics"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "kompicsjava/target/scala-2.12/Kompics Java Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompics"), javaBin, javaOpts ++ Seq[Shellable]("-jar", "kompicsjava/target/scala-2.12/Kompics Java Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("kompics/kompicsjava/target/scala-2.12/Kompics Java Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar"))
	),
	"KOMPACTAC" -> BenchmarkImpl(
		symbol="KOMPACTAC",
		label="Kompact Actor",
		local = (benchRunnerAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("actor", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("actor", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("actor", benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("kompact/target/release/kompact_benchmarks"), relp("kompact/configs"))
	),
	"KOMPACTCO" -> BenchmarkImpl(
		symbol="KOMPACTCO",
		label="Kompact Component",
		local = (benchRunnerAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("component", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("component", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("component", benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("kompact/target/release/kompact_benchmarks"), relp("kompact/configs"))
	),
	"KOMPACTMIX" -> BenchmarkImpl(
		symbol="KOMPACTMIX",
		label="Kompact Mixed",
		local = (benchRunnerAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("mixed", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("mixed", benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("kompact"), relp("kompact/target/release/kompact_benchmarks"), Seq("mixed", benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("kompact/target/release/kompact_benchmarks"), relp("kompact/configs"))
	),
	"ACTIX" -> BenchmarkImpl(
		symbol="ACTIX",
		label="Actix",
		local = (benchRunnerAddr) => Runner(relp("actix"), relp("actix/target/release/actix_benchmarks"), Seq(benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("actix"), relp("actix/target/release/actix_benchmarks"), Seq(benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("actix"), relp("actix/target/release/actix_benchmarks"), Seq(benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("actix/target/release/actix_benchmarks"))
	),
	"ERLANG" -> BenchmarkImpl(
		symbol="ERLANG",
		label="Erlang",
		local = (benchRunnerAddr) => Runner(relp("erlang"), relp("erlang/run.sh"), Seq(s"erlang${benchRunnerAddr.port}@${benchRunnerAddr.address}", "-erlang_benchmarks", "runner", s""""${benchRunnerAddr}"""")),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("erlang"), relp("erlang/run.sh"), Seq(s"erlang${benchMasterAddr.port}@${benchMasterAddr.address}", "-erlang_benchmarks", "runner", s""""${benchRunnerAddr}"""", "master", s""""${benchMasterAddr}"""", "clients", numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("erlang"), relp("erlang/run.sh"), Seq(s"erlang${benchClientAddr.port}@${benchClientAddr.address}", "-erlang_benchmarks", "master", s""""${benchMasterAddr}"""", "client", s""""${benchClientAddr}"""")),
		mustCopy = List(relp("erlang/run.sh"), relp("erlang/_rel"))
	),
	"RIKER" -> BenchmarkImpl(
		symbol="RIKER",
		label="Riker",
		local = (benchRunnerAddr) => Runner(relp("riker"), relp("riker/target/release/riker_benchmarks"), Seq(benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr, numClients) => Runner(relp("riker"), relp("riker/target/release/riker_benchmarks"), Seq(benchRunnerAddr, benchMasterAddr, numClients)),
		client = (benchMasterAddr, benchClientAddr) => Runner(relp("riker"), relp("riker/target/release/riker_benchmarks"), Seq(benchMasterAddr, benchClientAddr)),
		mustCopy = List(relp("riker/target/release/riker_benchmarks"))
	)
);

implicit class AddressArgImpl(arg: AddressArg) {
	private lazy val (first, second) = {
		val split = arg.split(":");
		assert(split.length == 2);
		val addr = split(0);
		val port = split(1).toInt;
		(addr, port)
	};

	def address: String = first;
	def port: Int = second;
}
