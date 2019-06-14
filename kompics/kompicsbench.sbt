ThisBuild / organization := "se.kth.benchmarks"

ThisBuild / version := "0.2.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / resolvers ++= Seq(Resolver.mavenLocal, Resolver.jcenterRepo, Resolver.bintrayRepo("kompics", "Maven"))

val kompicsV = "1.1.0";
val kompicsScala1xV = "1.1.0";
val kompicsScala2xV = "2.0.0";

lazy val commonSettings = Seq(
  fork := true,
  parallelExecution := false,
  scalacOptions ++= Seq(
    "-deprecation"
  ),
  javacOptions ++= Seq(
    "-Xlint:deprecation"
  ),
	"ch.qos.logback" % "logback-classic" % "1.2.3",
    "se.sics.kompics" %% "kompics-scala" % kompicsV,
    "se.sics.kompics" % "kompics-core" % kompicsV,
    "se.sics.kompics.basic" % "kompics-component-netty-network" % kompicsV,
    "se.sics.kompics.basic" % "kompics-port-network" % kompicsV,
    "se.sics.kompics.basic" % "kompics-component-java-timer" % kompicsV,
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
