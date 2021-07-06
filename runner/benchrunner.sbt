name := "Benchmark Suite Runner"

organization in ThisBuild := "se.kth.benchmarks"

version in ThisBuild := "0.3.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.9"

resolvers += Resolver.mavenLocal
resolvers += Resolver.bintrayRepo("lkrollcom", "maven")

libraryDependencies ++= Seq(
	"se.kth.benchmarks" %% "benchmark-suite-shared" % "1.0.1-SNAPSHOT",
    "com.lkroll" %% "common-data-tools" % "1.+",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    "org.rogach" %% "scallop" % "3.1.2",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test",
    "com.panayotis.javaplot" % "javaplot" % "0.5.0" % "provided"
    //"com.thesamet.scalapb" %% "scalapb-runtime-grpc" % "0.8.2"
)

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}
