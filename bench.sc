#!/usr/bin/env amm

import ammonite.ops._
import ammonite.ops.ImplicitWd._
import scala.concurrent.duration._
//import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
//import java.lang.{Process, ProcessBuilder}
import java.io.{PrintWriter, OutputStream, File, FileWriter}
import $file.build, build.{relps, relp, binp, format}
import $file.benchmarks, benchmarks._
import $ivy.`com.decodified::scala-ssh:0.9.0`, com.decodified.scalassh.{SSH, HostConfigProvider, PublicKeyLogin}
//import $ivy.`ch.qos.logback:logback-classic:1.1.7`

val runnerAddr = "127.0.0.1:45678";
val masterAddr = "192.168.0.106:45679";

def getExperimentRunner(prefix: String, results: Path): BenchmarkRunner = BenchmarkRunner(
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

val logs = pwd / 'logs;
val results = pwd / 'results;
val defaultNodesFile = pwd / "nodes.conf";

@doc("Run a specific benchmark client.")
@main
def client(name: String, master: AddressArg, runid: String, publicif: String): Unit = {
	val runId = runid;
	val publicIf = publicif;
	implementations.get(name) match {
		case Some(impl) => {
			println(s"Found Benchmark ${impl.label} for ${name}. Master is at $master");
			val logdir = logs / runId;
			mkdir! logdir;
			val clientRunner = impl.clientRunner(master, s"${publicIf}:45678");
			val client = clientRunner.run(logdir);
			client.waitFor();
			Console.err.println("Client shut down!");
		}
		case None => {
			Console.err.println(s"No Benchmark Implementation found for '${name}'"); 
			System.exit(1);
		}
	}
}

@doc("Run benchmarks using a cluster of nodes.")
@main
def remote(withNodes: Path = defaultNodesFile, test: String = ""): Unit = {
	val nodes = readNodes(withNodes);
	val masters = implementations.values.filter(_.symbol.startsWith(test)).map(_.remoteRunner(runnerAddr, masterAddr, nodes.size));
	val totalStart = System.currentTimeMillis();
	val runId = s"run-${totalStart}";
	val logdir = logs / runId;
	mkdir! logdir;
	val resultsdir = results / runId;
	mkdir! resultsdir;
	val nRunners = masters.size;
	var errors = 0;
	masters.zipWithIndex.foreach { case (master, i) =>
		val experimentRunner = getExperimentRunner(master.symbol, resultsdir);
		println(s"Starting run [${i+1}/$nRunners]: ${master.label}");
		val start = System.currentTimeMillis();
		val r = remoteExperiment(experimentRunner, master, runId, logdir, nodes);
		val end = System.currentTimeMillis();
		val time = FiniteDuration(end-start, MILLISECONDS);
		r match {
			case Success(_) => println(s"Finished ${master.label} in ${format(time)}");
			case Failure(e) => {
				errors += 1;
				println(s"Runner did not finish successfully: ${master.label} (${format(time)})");
				Console.err.println(e);
				e.printStackTrace(Console.err);
			}
		}
		endSeparator(master.label, experimentRunner.errorLog(logdir));
		endSeparator(master.label, experimentRunner.outputLog(logdir));
	}
	val totalEnd = System.currentTimeMillis();
	val totalTime = FiniteDuration(totalEnd-totalStart, MILLISECONDS);
	println("========");
	println(s"Finished all runners in ${format(totalTime)}");
	println(s"There were $errors errors. Logs can be found in ${logdir}");
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

private def remoteExperiment(experimentRunner: BenchmarkRunner, master: BenchmarkRunner, runId: String, logDir: Path, nodes: List[NodeEntry]): Try[Unit] = {
	Try {
		val runner = master.run(logDir);
		val pids = nodes.map { node => 
			val pid = startClient(node, master.symbol, runId, masterAddr);
			(node -> pid)
		};
		println(s"Got pids: $pids");
		val experimenter = experimentRunner.run(logDir);
		experimenter.waitFor();
		runner.destroy();
		pids.foreach {
			case (node, Success(pid)) => {
				val r = stopClient(node, pid);
				println(s"Tried to stop client $node: $r");
			}
			case(node, Failure(_)) => Console.err.println(s"Could not stop client $node due to missing pid")
		}
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

private def startClient(node: NodeEntry, bench: String, runId: String, master: String): Try[Int] = {
	println(s"Connecting to ${node}...");
	val connRes = SSH(node.ip, login) { client =>
		for {
			r <- client.exec(s"source ~/.profile; cd ${node.benchDir}; ./client.sh --name $bench --master $master --runid $runId --publicif ${node.ip}");
			pid <- Try(r.stdOutAsString().trim.toInt)
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
