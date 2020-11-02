import sbtassembly.AssemblyPlugin.autoImport.assemblyShadeRules

ThisBuild / organization := "se.kth.benchmarks"

ThisBuild / version := "0.2.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.9"

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
  test in assembly := {},
/*  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("io.netty.netty-all.**" -> "shadeio.n-all.@1")
      .inLibrary("se.kth.benchmarks" %% "benchmark-suite-shared" % "1.0.1-SNAPSHOT")
      .inAll
  ),*/
  assemblyMergeStrategy in assembly := {
    case "META-INF/io.netty.versions.properties" => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
);

lazy val root = (project in file("."))
  .aggregate(kompicsJava, kompicsScala1x, kompicsScala2x)
  .settings(
    commonSettings,
    name := "Kompics Benchmark Suite (Root)",
    skip in assembly := true
  );

lazy val shared = (project in file("shared"))
  .settings(
    commonSettings,
    name := "Kompics Benchmark Suite (Shared)",
    libraryDependencies ++= Seq(
      "se.kth.benchmarks" %% "benchmark-suite-shared" % "1.0.1-SNAPSHOT" excludeAll (
        ExclusionRule(organization = "io.netty")
        ),
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "se.sics.kompics" % "kompics-core" % kompicsV,
      "se.sics.kompics.basic" % "kompics-component-netty-network" % kompicsV,
      "se.sics.kompics.basic" % "kompics-port-network" % kompicsV,
      "se.sics.kompics.basic" % "kompics-component-java-timer" % kompicsV,
      "org.scalatest" %% "scalatest" % "3.0.8" % "test",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )
  );

lazy val kompicsJava = (project in file("kompicsjava"))
  .dependsOn(shared % "test->test;compile->compile")
  .settings(
    commonSettings,
    name := "Kompics Java Benchmark Suite",
    libraryDependencies ++= Seq(
      "se.sics.kompics" %% "kompics-scala" % kompicsScala1xV // needed for SystemProvider
    )
  );

lazy val kompicsScala1x = (project in file("kompicsscala1x"))
  .dependsOn(shared % "test->test;compile->compile")
  .settings(
    commonSettings,
    name := "Kompics Scala 1.x Benchmark Suite",
    libraryDependencies ++= Seq(
      "se.sics.kompics" %% "kompics-scala" % kompicsScala1xV
    )
  );

lazy val kompicsScala2x = (project in file("kompicsscala2x"))
  .dependsOn(shared % "test->test;compile->compile")
  .settings(
    commonSettings,
    name := "Kompics Scala 2.x Benchmark Suite",
    libraryDependencies ++= Seq(
      "se.sics.kompics" %% "kompics-scala" % kompicsScala2xV
    )
  );

// assemblyShadeRules in assembly := Seq(
//   ShadeRule.rename("io.netty.**" -> "custom_netty.@1")
//   .inLibrary("io.netty" % "netty-all" % "5.0.0.Alpha3")
//   .inLibrary("se.sics.kompics.basic" % "kompics-component-netty-network" % kompicsV)
//   .inProject
// )

//logLevel in assembly := Level.Debug