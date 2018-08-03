#!/usr/bin/env amm

import ammonite.ops._
import ammonite.ops.ImplicitWd._
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
import java.lang.{Process, ProcessBuilder}
import java.io.{PrintWriter, OutputStream, File, FileWriter}
import $file.build
import build.{relps, relp, binp, format}
import $ivy.`com.decodified::scala-ssh:0.9.0`, com.decodified.scalassh.{SSH, HostConfigProvider, PublicKeyLogin}
//import $ivy.`ch.qos.logback:logback-classic:1.1.7`

type AddressArg = String;
type LocalRunner = (AddressArg) => Runner;
type RemoteRunner = (AddressArg, AddressArg) => Runner;
type ClientRunner = (AddressArg, AddressArg, AddressArg) => Runner;

case class BenchmarkImpl(symbol: String, label: String, local: LocalRunner, remote: RemoteRunner, client: ClientRunner) {
	def localRunner(benchRunnerAddr: AddressArg): BenchmarkRunner = 
		BenchmarkRunner(info, local(benchRunnerAddr));
	def remoteRunner(benchRunnerAddr: AddressArg, benchMasterAddr: AddressArg): BenchmarkRunner = 
		BenchmarkRunner(info, remote(benchRunnerAddr, benchMasterAddr));
	def clientRunner(benchRunnerAddr: AddressArg, benchMasterAddr: AddressArg, benchClientAddr: AddressArg): BenchmarkRunner = 
		BenchmarkRunner(info, client(benchRunnerAddr, benchMasterAddr, benchClientAddr));
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
		pb.directory(runner.env.toIO);
		pb.redirectError(ProcessBuilder.Redirect.appendTo(errorLog(logFolder)));
		pb.redirectOutput(ProcessBuilder.Redirect.appendTo(outputLog(logFolder)));
		pb.start();
	}
	lazy val fileLabel: String = bench.label.toLowerCase().replaceAll(" ", "_");
	def outputLog(logFolder: Path) = (logFolder / s"${fileLabel}.out").toIO;
	def errorLog(logFolder: Path) = (logFolder / s"${fileLabel}.error").toIO;
}

val runnerAddr = "127.0.0.1:45678";

val javaBin = binp('java);

def getExperimentRunner(prefix: String, results: Path) = BenchmarkRunner(
	bench = BenchmarkInfo(
		"RUN",
		"Experiment Runner"), 
	runner = Runner(
		relp("runner"), 
		javaBin, 
		Seq("-jar", 
			"target/scala-2.12/Benchmark Suite Runner-assembly-0.2.0-SNAPSHOT.jar",
			"--server", runnerAddr,
			"--prefix", prefix,
			"--output-folder", results.toString)
	)
);

val implementations: Map[String, BenchmarkImpl] = Map(
	"AKKA" -> BenchmarkImpl(
		symbol="AKKA", 
		label="Akka", 
		local = (benchRunnerAddr) => Runner(relp("akka"), javaBin, Seq("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr) => Runner(relp("akka"), javaBin, Seq("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr, benchMasterAddr)),
		client = (benchRunnerAddr, benchMasterAddr, benchClientAddr) => Runner(relp("akka"), javaBin, Seq("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.2.0-SNAPSHOT.jar", benchRunnerAddr, benchMasterAddr, benchClientAddr))
	),
	"KOMPSC" -> BenchmarkImpl(
		symbol="KOMPSC", 
		label="Kompics Scala", 
		local = (benchRunnerAddr) => Runner(relp("kompics_scala"), javaBin, Seq("-jar", "target/scala-2.12/Kompics Benchmark Suite-assembly-0.1.0-SNAPSHOT.jar", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr) => Runner(relp("kompics_scala"), javaBin, Seq("-jar", "target/scala-2.12/Kompics Benchmark Suite-assembly-0.1.0-SNAPSHOT.jar", benchRunnerAddr, benchMasterAddr)),
		client = (benchRunnerAddr, benchMasterAddr, benchClientAddr) => Runner(relp("kompics_scala"), javaBin, Seq("-jar", "target/scala-2.12/Kompics Benchmark Suite-assembly-0.1.0-SNAPSHOT.jar", benchRunnerAddr, benchMasterAddr, benchClientAddr))
	),
	"KRUSTAC" -> BenchmarkImpl(
		symbol="KRUSTAC", 
		label="Kompics Rust Actor", 
		local = (benchRunnerAddr) => Runner(relp("kompics_rust"), relp("kompics_rust/target/release/kompics_rust_benchmarks"), Seq("actor", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr) => Runner(relp("kompics_rust"), relp("kompics_rust/target/release/kompics_rust_benchmarks"), Seq("actor", benchRunnerAddr, benchMasterAddr)),
		client = (benchRunnerAddr, benchMasterAddr, benchClientAddr) => Runner(relp("kompics_rust"), relp("kompics_rust/target/release/kompics_rust_benchmarks"), Seq("actor", benchRunnerAddr, benchMasterAddr, benchClientAddr))
	),
	"KRUSTCO" -> BenchmarkImpl(
		symbol="KRUSTCO", 
		label="Kompics Rust Component", 
		local = (benchRunnerAddr) => Runner(relp("kompics_rust"), relp("kompics_rust/target/release/kompics_rust_benchmarks"), Seq("component", benchRunnerAddr)),
		remote = (benchRunnerAddr, benchMasterAddr) => Runner(relp("kompics_rust"), relp("kompics_rust/target/release/kompics_rust_benchmarks"), Seq("component", benchRunnerAddr, benchMasterAddr)),
		client = (benchRunnerAddr, benchMasterAddr, benchClientAddr) => Runner(relp("kompics_rust"), relp("kompics_rust/target/release/kompics_rust_benchmarks"), Seq("component", benchRunnerAddr, benchMasterAddr, benchClientAddr))
	),
);

val logs = pwd / 'logs;
val results = pwd / 'results;
val defaultNodesFile = pwd / "nodes.conf";

@doc("Run a specific benchmark client.")
@main
def client(name: String, master: AddressArg): Unit = {
	implementations.get(name) match {
		case Some(i) => {
			println(s"Found Benchmark ${i.label} for ${name}. Master is at $master");
			Thread.sleep(100000);
			println("Finished waiting.");
		}
		case None => {
			Console.err.println(s"No Benchmark Implementation found for '${name}'"); 
			System.exit(1);
		}
	}
}

@doc("Run benchmarks using a cluster of nodes.")
@main
def remote(withNodes: Path = defaultNodesFile): Unit = {
	val nodes = readNodes(withNodes);
	val pids = nodes.map(node => (node -> startClient(node, "AKKA", "192.168.251.1:45678")));
	println(s"Got pids: $pids");
	pids.foreach {
		case (node, Success(pid)) => {
			val r = stopClient(node, pid);
			println(s"Tried to stop client $node: $r");
		}
		case(node, Failure(_)) => Console.err.println(s"Could not stop client $node due to missing pid")
	}
}

@doc("Run local benchmarks only.")
@main
def local(): Unit = {
	val runners = implementations.values.map(_.localRunner(runnerAddr));
	val totalStart = System.currentTimeMillis();
	val runId = s"run-${totalStart}";
	val logdir = logs / runId;
	mkdir! logdir;
	val resultsdir = results / runId;
	mkdir! resultsdir;
	val nRunners = runners.size;
	var errors = 0;
	runners.zipWithIndex.foreach { case (r, i) =>
		try {
			val experimentRunner = getExperimentRunner(r.symbol, resultsdir);
			println(s"Starting run [${i+1}/$nRunners]: ${r.label}");
			val start = System.currentTimeMillis();
			val runner = r.run(logdir);
			val experimenter = experimentRunner.run(logdir);
			experimenter.waitFor();
			runner.destroy();
			val end = System.currentTimeMillis();
			val time = FiniteDuration(end-start, MILLISECONDS);
			endSeparator(r.label, experimentRunner.errorLog(logdir));
			endSeparator(r.label, experimentRunner.outputLog(logdir));
			if (experimenter.exitValue() == 0) {
				println(s"Finished ${r.label} in ${format(time)}");
			} else {
				errors += 1;
				println(s"Runner did not finish successfully: ${r.label} (${format(time)})");
			}
		} catch {
			case e: Throwable => e.printStackTrace(Console.err);
		}
	}
	val totalEnd = System.currentTimeMillis();
	val totalTime = FiniteDuration(totalEnd-totalStart, MILLISECONDS);
	println("========");
	println(s"Finished all runners in ${format(totalTime)}");
	println(s"There were $errors errors. Logs can be found in ${logdir}");
}

private def endSeparator(label: String, log: File): Unit = {
	val fw = new FileWriter(log, true);
	val w = new PrintWriter(fw);
	try {
		w.println(s"===== END $label =====");
	} finally {
		w.flush();
		w.close();
		fw.close();
	}
}

case class NodeEntry(ip: String, benchDir: String)

private def readNodes(p: Path): List[NodeEntry] = {
	if (exists! p) {
		println(s"Reading nodes from '${p}'");
		val nodesS = read! p;
		val nodeLines = nodesS.split("\n").filterNot(_.contains("#")).toList;
		nodeLines.map { l => 
			val ls = l.split("""\s\|\s""");
			println(s"Got ${ls.mkString}");
			assert(ls.size == 2);
			val node = ls(0).trim;
			val path = ls(1).trim;
			NodeEntry(node, path)
		}
	} else {
		Console.err.println(s"Could not find nodes config file '${p}'");
		System.exit(1);
		???
	}
}

val login = HostConfigProvider.fromLogin(PublicKeyLogin("lkroll", "/Users/lkroll/.ssh/id_rsa"));

private def startClient(node: NodeEntry, bench: String, master: String): Try[Int] = {
	println(s"Connecting to ${node}...");
	val connRes = SSH(node.ip, login) { client =>
		for {
			r <- client.exec(s"cd ${node.benchDir}; bench.sc client --name $bench --master $master");
			pid <- Try(r.stdOutAsString().toInt)
		} yield pid
	};
	println(s"Connection: $connRes");
	connRes
}

private def stopClient(node: NodeEntry, pid: Int): Try[Unit] = {
	println(s"Connecting to ${node}...");
	val connRes = SSH(node.ip, login) { client =>
		for {
			r <- client.exec(s"kill -5 $pid")
		} yield ()
	};
	println(s"Connection: $connRes");
	connRes
}