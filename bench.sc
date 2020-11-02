#!/usr/bin/env amm

import ammonite.ops._
import ammonite.ops.ImplicitWd._
import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
//import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
//import java.lang.{Process, ProcessBuilder}
import java.io.{PrintWriter, OutputStream, File, FileWriter}
import java.nio.file.Files
import $file.build, build.{relps, relp, binp, format}
import $file.benchmarks, benchmarks._
import $ivy.`com.decodified::scala-ssh:0.10.0`, com.decodified.scalassh.{SSH, HostConfigProvider, PublicKeyLogin}
//import $ivy.`ch.qos.logback:logback-classic:1.1.7`

val runnerAddr = "127.0.0.1:45678";
//val masterAddr = "192.168.0.106:45679";
val masterAddr = "127.0.0.1:45679";

def getExperimentRunner(prefix: String, results: Path, testing: Boolean, benchmarks_string: String): BenchmarkRunner = {
	var params: Seq[Shellable] = Seq(
		"-jar",
		"target/scala-2.12/Benchmark Suite Runner-assembly-0.3.0-SNAPSHOT.jar",
		"--server", runnerAddr,
		"--prefix", prefix,
		"--output-folder", results.toString);
	if (testing) {
		params ++= Seq[Shellable]("--testing");
	}
	if (!benchmarks_string.isEmpty) {
		params ++= Seq[Shellable]("--benchmarks", benchmarks_string);
	}
	BenchmarkRunner(
		bench = BenchmarkInfo(
			"RUN",
			"Experiment Runner"),
		runner = Runner(
			relp("runner"),
			javaBin,
			params
		)
	)
};

val logs = pwd / 'logs;
val results = pwd / 'results;
val defaultNodesFile = pwd / "nodes.conf";

@arg(doc ="Run a specific benchmark client.")
@main
def client(name: String, master: AddressArg, runid: String, publicif: String, clientPort: Int = 45678): Unit = {
	val runId = runid;
	val publicIf = publicif;
	implementations.get(name) match {
		case Some(impl) => {
			println(s"Found Benchmark ${impl.label} for ${name}. Master is at $master");
			val logdir = logs / runId;
			mkdir! logdir;
			val clientRunner = impl.clientRunner(master, s"${publicIf}:${clientPort}");
			val client = clientRunner.run(logdir);
			Runtime.getRuntime().addShutdownHook(new Thread() {
		      override def run(): Unit = {
		        println("Got termination signal. Killing client.");
		        client.destroy();
		      }
		    });
			client.waitFor();
			Console.err.println("Client shut down with code="+client.exitValue()+"!");
		}
		case None => {
			Console.err.println(s"No Benchmark Implementation found for '${name}'");
			System.exit(1);
		}
	}
}

@arg(doc ="Run benchmarks using a cluster of nodes.")
@main
def remote(withNodes: Path = defaultNodesFile, testing: Boolean = false, impls: String = "", benchmarks: String = ""): Unit = {
	val nodes = readNodes(withNodes);
	val masters = runnersForImpl(impls, _.remoteRunner(runnerAddr, masterAddr, nodes.size));
	val totalStart = System.currentTimeMillis();
	val runId = s"run-${totalStart}";
	val logdir = logs / runId;
	mkdir! logdir;
	val resultsdir = results / runId;
	mkdir! resultsdir;
	if (!testing) {
		%%.apply(root/'bin/'bash, "-c",s"./exp_setup.sh $resultsdir");
	}
	val nRunners = masters.size;
	var errors = 0;
	masters.zipWithIndex.foreach { case (master, i) =>
		val experimentRunner = getExperimentRunner(master.symbol, resultsdir, testing, benchmarks);
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

@arg(doc ="Run benchmarks using a cluster of nodes.")
@main
def fakeRemote(withClients: Int = 1, testing: Boolean = false, impls: String = "", benchmarks: String = "", remoteDir: os.Path = tmp.dir()): Unit = {
	val alwaysCopyFiles = List[Path](relp("bench.sc"), relp("benchmarks.sc"), relp("build.sc"), relp("client.sh"));
	val masterBenches = runnersForImpl(impls, identity);
	val (copyFiles: List[RelPath], copyDirectories: List[RelPath]) = masterBenches.map(_.mustCopy).flatten.distinct.partition(_.isFile) match {
		case (files, folders) => ((files ++ alwaysCopyFiles).map(_.relativeTo(pwd)), folders.map(_.relativeTo(pwd)))
	};
	println(s"Going to copy files=${copyFiles.mkString("[", ",", "]")} and folders==${copyDirectories.mkString("[", ",", "]")}.");
	val totalStart = System.currentTimeMillis();
	val runId = s"run-${totalStart}";
	val nodes = (0 until withClients).map(45700 + _).map { p =>
		val ip = "127.0.0.1";
		val addr = s"${ip}:${p}";
		val dirName = s"${ip}-port-${p}";
		val dir = remoteDir / runId / dirName;
		print(s"Created temporary directory for test node $addr: ${dir}, copying data...");
		for (d <- copyDirectories) {
			mkdir(dir / d);
			cp.over(pwd / d, dir / d);
		}
		for (file <- copyFiles) {
			os.copy(pwd / file, dir / file, createFolders = true);
		}
		println("done.");
		NodeEntry(ip, p, dir.toString)
	} toList;
	val masters = masterBenches.map(_.remoteRunner(runnerAddr, masterAddr, nodes.size));
	val logdir = logs / runId;
	mkdir! logdir;
	val resultsdir = results / runId;
	mkdir! resultsdir;
	if (!testing) {
		%%.apply(root/'bin/'bash, "-c",s"./exp_setup.sh $resultsdir");
	}
	val nRunners = masters.size;
	var errors = 0;
	masters.zipWithIndex.foreach { case (master, i) =>
		val experimentRunner = getExperimentRunner(master.symbol, resultsdir, testing, benchmarks);
		println(s"Starting run [${i+1}/$nRunners]: ${master.label}");
		val start = System.currentTimeMillis();
		val r = fakeRemoteExperiment(experimentRunner, master, runId, logdir, nodes);
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
	println(s"Run the following command to cleanup when remote logs are no longer required:\n	rm -rf $remoteDir");
}


@arg(doc ="Run local benchmarks only.")
@main
def local(testing: Boolean = false, impls: String = "", benchmarks: String = ""): Unit = {
	val runners = runnersForImpl(impls, _.localRunner(runnerAddr));
	val totalStart = System.currentTimeMillis();
	val runId = s"run-${totalStart}";
	val logdir = logs / runId;
	mkdir! logdir;
	val resultsdir = results / runId;
	mkdir! resultsdir;
	if (!testing) {
		%%.apply(root/'bin/'bash, "-c",s"./exp_setup.sh $resultsdir");
	}
	val nRunners = runners.size;
	var errors = 0;
	runners.zipWithIndex.foreach { case (r, i) =>
		try {
			val experimentRunner = getExperimentRunner(r.symbol, resultsdir, testing, benchmarks);
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

private def runnersForImpl[T](impl_string: String, mapper: BenchmarkImpl => T): List[T] = {
	val impls: Array[String] = impl_string.split(",")
	val runners: List[T] = if (impls.isEmpty) {
		implementations.values.map(mapper).toList;
	} else {
		impls.map(_.toUpperCase).flatMap(impl => {
			val res: Option[BenchmarkImpl] = implementations.get(impl);
			if (res.isEmpty) {
				Console.err.println(s"No benchmark found for impl ${impl}!");
			}
			res.map(mapper)
		}).toList
	};
	if (runners.isEmpty) {
		Console.err.println(s"No benchmarks found!");
		System.exit(1);
	}
	runners
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

private def fakeRemoteExperiment(experimentRunner: BenchmarkRunner, master: BenchmarkRunner, runId: String, logDir: Path, nodes: List[NodeEntry]): Try[Unit] = {
	Try {
		val runner = master.run(logDir);
		val pids = nodes.map { node =>
			val pid = startFakeClient(node, master.symbol, runId, masterAddr);
			(node -> pid)
		};
		println(s"Got pids: $pids");
		val experimenter = experimentRunner.run(logDir);
		experimenter.waitFor();
		runner.destroy();
		pids.foreach {
			case (node, Success(pid)) => {
				val r = stopFakeClient(node, pid);
				println(s"Tried to stop client $node: $r");
			}
			case(node, Failure(_)) => Console.err.println(s"Could not stop client $node due to missing pid")
		}
	}
}

case class NodeEntry(ip: String, port: Int, benchDir: String)

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
			NodeEntry(node, 45678, path)
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
			r <- client.exec(s"source ~/.profile; cd ${node.benchDir}; ./client.sh --name $bench --master $master --runid $runId --publicif ${node.ip} --clientPort ${node.port}");
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
			r <- client.exec(s"kill -15 $pid")
		} yield ()
	};
	println(s"Connection: $connRes");
	connRes
}

private def startFakeClient(node: NodeEntry, bench: String, runId: String, master: String): Try[Int] = {
	println(s"Starting ${node}...");
	Try {
		val wd = Path(node.benchDir);
		val res = %%.apply(root/'bin/'bash, "-c", s"./client.sh --name $bench --master $master --runid $runId --publicif ${node.ip} --clientPort ${node.port}")(wd);
		val connRes = res.out.string;
		println(s"Result: $connRes");
		connRes.trim.toInt
	}
}

private def stopFakeClient(node: NodeEntry, pid: Int): Try[Unit] = {
	println(s"Killing ${node}...");
	Try {
		val res = %%.apply(root/'bin/'bash, "-c",s"kill -15 $pid");
		val connRes = res.out.string;
		println(s"Connection: $connRes");
	}
}
