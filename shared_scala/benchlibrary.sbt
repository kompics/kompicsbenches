name := "Benchmark Suite Shared"

organization in ThisBuild := "se.kth.benchmarks"

version in ThisBuild := "1.0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.9"

resolvers += Resolver.mavenLocal

PB.protoSources in Compile := Seq(baseDirectory.value / "../proto/")

libraryDependencies ++= Seq(
  //"com.thesamet.scalapb" %% "compilerplugin" % "0.7.4",
  "io.grpc" % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.+",
  "com.github.hipjim" %% "scala-retry" % "0.3.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "test"
)

fork := true;

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)