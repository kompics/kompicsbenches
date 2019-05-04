name := "Benchmark Suite Shared"

organization in ThisBuild := "se.kth.benchmarks"

version in ThisBuild := "1.0.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.8"

resolvers += Resolver.mavenLocal

PB.protoSources in Compile := Seq(baseDirectory.value / "../proto/")

libraryDependencies ++= Seq(
	//"com.thesamet.scalapb" %% "compilerplugin" % "0.7.4",
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % "3.+",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)
