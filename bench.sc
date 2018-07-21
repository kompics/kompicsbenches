#!/usr/bin/env amm

import ammonite.ops._
import ammonite.ops.ImplicitWd._
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import java.lang.{Process, ProcessBuilder}
import java.io.{PrintWriter, OutputStream, File, FileWriter}
import $file.build
import build.{relps, relp, binp, format}

case class Runner(label: String, env: Path, exec: Path, args: Seq[Shellable]) {
	def run(logFolder: Path): Process = {
		val command = (exec.toString +: args.flatMap(_.s)).toList.asJava;
		val pb = new ProcessBuilder(command);
		pb.directory(env.toIO);
		pb.redirectError(ProcessBuilder.Redirect.appendTo(errorLog(logFolder)));
		pb.redirectOutput(ProcessBuilder.Redirect.appendTo(outputLog(logFolder)));
		pb.start();
	}
	lazy val fileLabel: String = label.toLowerCase().replaceAll(" ", "_");
	def outputLog(logFolder: Path) = (logFolder / s"${fileLabel}.out").toIO;
	def errorLog(logFolder: Path) = (logFolder / s"${fileLabel}.error").toIO;
}

val runnerAddr = "127.0.0.1:45678";

val javaBin = binp('java);

val experimentRunner = Runner("Experiment Runner", relp("runner"), javaBin, Seq("-jar", "target/scala-2.12/Benchmark Suite Runner-assembly-0.1.0-SNAPSHOT.jar"));

val runners: List[Runner] = List(
	Runner("Akka", relp("akka"), javaBin, Seq("-jar", "target/scala-2.12/Akka Benchmark Suite-assembly-0.1.0-SNAPSHOT.jar", runnerAddr)),
	Runner("Kompics Rust", relp("kompics_rust"), relp("kompics_rust/target/release/kompics_rust_benchmarks"), Seq(runnerAddr))
);

val logs = pwd / 'logs;

@main
def main(): Unit = {
	val totalStart = System.currentTimeMillis();
	val logdir = logs / s"run-${totalStart}";
	mkdir! logdir;
	val nRunners = runners.size;
	var errors = 0;
	runners.zipWithIndex.foreach { case (r, i) =>
		try {
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