name := "Akka Benchmark Suite"

organization in ThisBuild := "se.kth.benchmarks"

version in ThisBuild := "0.2.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.6"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
	"se.kth.benchmarks" %% "benchmark-suite-shared" % "1.0.0-SNAPSHOT",
    "com.typesafe.akka" %% "akka-actor" % "2.5.14"
)

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}