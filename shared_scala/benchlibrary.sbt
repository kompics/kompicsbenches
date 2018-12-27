name := "Benchmark Suite Shared"

organization in ThisBuild := "se.kth.benchmarks"

version in ThisBuild := "1.0.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.6"

resolvers += Resolver.mavenLocal

PB.protoSources in Compile := Seq(baseDirectory.value / "../proto/")

libraryDependencies ++= Seq(
	//"com.thesamet.scalapb" %% "compilerplugin" % "0.7.4",
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "com.typesafe.akka" %% "akka-actor" % "2.5.14",
    "com.typesafe.akka" %% "akka-remote" % "2.5.14",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.+"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)
