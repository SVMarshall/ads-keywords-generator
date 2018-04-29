import sbt._
import Keys._

val scioVersion = "0.4.7"
val beamVersion = "2.2.0"
val scalaMacrosVersion = "2.1.0"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization          := "deepmarketing",
  // Semantic versioning http://semver.org/
  version               := "0.1.0-SNAPSHOT",
  scalaVersion          := "2.11.12",
  scalacOptions         ++= Seq("-target:jvm-1.8",
                                "-deprecation",
                                "-feature",
                                "-unchecked"),
  javacOptions          ++= Seq("-source", "1.8",
                                "-target", "1.8")
  //resolvers += "mvnrepository.com" at "http://central.maven.org/maven2/"
)

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  addCompilerPlugin(paradiseDependency)
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

//lazy val utils = project
//  .settings(
//    commonSettings ++ macroSettings ++ noPublishSettings,
//    name := "utils",
//    libraryDependencies ++= Seq(
//      "com.google.api-client" % "google-api-client" % "1.23.0",
//      "com.google.oauth-client" % "google-oauth-client-jetty" % "1.23.0",
//      "com.google.apis" % "google-api-services-sheets" % "v4-rev491-1.23.0"
//    )
//  )

lazy val root: Project = Project("structure-generator", file("."))
  .settings(
    commonSettings ++ macroSettings ++ noPublishSettings,
    description := "structure-generator",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % "test",
      "com.spotify" %% "scio-repl" % scioVersion,
      // optional direct runner
      //"org.apache.beam" % "beam-runners-spark" % beamVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      // optional dataflow runner
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.slf4j" % "slf4j-simple" % "1.7.25",
      "com.google.api-client" % "google-api-client" % "1.23.0",
      "com.google.oauth-client" % "google-oauth-client-jetty" % "1.23.0",
      "com.google.apis" % "google-api-services-sheets" % "v4-rev491-1.23.0"))
  //.dependsOn(utils)
  .enablePlugins(PackPlugin)

lazy val repl: Project = Project(
  "repl",
  file(".repl")
).settings(
  commonSettings ++ macroSettings ++ noPublishSettings,
  description := "Scio REPL for structure generator",
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-repl" % scioVersion
  ),
  mainClass in Compile := Some("com.spotify.scio.repl.ScioShell")
).dependsOn(
  root
)
