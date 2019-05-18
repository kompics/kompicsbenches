name := "Kompics Benchmark Suite"

organization in ThisBuild := "se.kth.benchmarks"

version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.6"

resolvers += Resolver.mavenLocal
resolvers += Resolver.jcenterRepo
resolvers += Resolver.bintrayRepo("kompics", "Maven")

val kompicsV = "1.1.0";

libraryDependencies ++= Seq(
	"se.kth.benchmarks" %% "benchmark-suite-shared" % "1.0.0-SNAPSHOT" excludeAll(
    ExclusionRule(organization = "io.netty")
  ),
	"ch.qos.logback" % "logback-classic" % "1.2.3",
    "se.sics.kompics" %% "kompics-scala" % kompicsV,
    "se.sics.kompics" % "kompics-core" % kompicsV,
    "se.sics.kompics.basic" % "kompics-component-netty-network" % kompicsV,
    "se.sics.kompics.basic" % "kompics-port-network" % kompicsV,
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
)

fork := true; // needed for UDT tests to clean up properly after themselves
parallelExecution in ThisBuild := false;
//test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// assemblyShadeRules in assembly := Seq(
//   ShadeRule.rename("io.netty.**" -> "custom_netty.@1")
//   .inLibrary("io.netty" % "netty-all" % "5.0.0.Alpha3")
//   .inLibrary("se.sics.kompics.basic" % "kompics-component-netty-network" % kompicsV)
//   .inProject
// )

//logLevel in assembly := Level.Debug
