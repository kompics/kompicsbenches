name := "Benchmark Suite Shared"

organization in ThisBuild := "se.kth.benchmarks"

version in ThisBuild := "1.0.0"

scalaVersion in ThisBuild := "2.12.6"

resolvers += Resolver.mavenLocal

PB.protoSources in Compile := Seq(baseDirectory.value / "../proto/")

libraryDependencies ++= Seq(
	"com.thesamet.scalapb" %% "compilerplugin" % "0.7.4",
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)