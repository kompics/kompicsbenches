name := "Akka Benchmark Suite"

organization in ThisBuild := "se.kth.benchmarks"

version in ThisBuild := "0.3.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.9"

resolvers += Resolver.mavenLocal

val akkaV = "2.5.25"

libraryDependencies ++= Seq(
  "se.kth.benchmarks" %% "benchmark-suite-shared" % "1.0.0-SNAPSHOT",
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-remote" % akkaV,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaV,
  "com.typesafe.akka" %% "akka-slf4j" % akkaV,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

fork := true;

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

parallelExecution in ThisBuild := false
