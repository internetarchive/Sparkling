import sbt.ExclusionRule
import sbt.Keys._

lazy val commonSettings = Seq(name := "sparkling", organization := "org.archive.webservices", version := "0.3.8-SNAPSHOT", scalaVersion := "2.12.8", fork := true)

val circeVersion = "0.13.0"

val guava = "com.google.guava" % "guava" % "29.0-jre"

val failureaccess = "com.google.guava" % "failureaccess" % "1.0.1"

val webarchiveCommons = "org.netpreserve.commons" % "webarchive-commons" % "1.1.8" excludeAll
  (
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-core"),
    ExclusionRule(organization = "org.apache.httpcomponents", name = "httpcore"),
    ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient"),
    ExclusionRule(organization = "joda-time", name = "joda-time")
  )

lazy val sparkling = (project in file(".")).settings(
  commonSettings,
  libraryDependencies ++= Seq(
    guava,
    failureaccess,
    "commons-codec" % "commons-codec" % "1.12",
    "org.apache.commons" % "commons-compress" % "1.14",
    "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
    "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
    "joda-time" % "joda-time" % "2.10",
    webarchiveCommons,
    "org.slf4j" % "slf4j-log4j12" % "1.7.26",
    "log4j" % "log4j" % "1.2.17",
    "net.debasishg" %% "redisclient" % "3.10" % "provided",
    "org.apache.ignite" % "ignite-core" % "2.7.6" % "provided",
    "edu.stanford.nlp" % "stanford-corenlp" % "4.3.1" % "provided",
    "org.brotli" % "dec" % "0.1.2",
    "sh.almond" %% "ammonite-spark" % "0.10.1" % "provided",
    ("com.lihaoyi" % "ammonite-interp" % "1.7.4" % "provided").cross(CrossVersion.full),
    ("com.lihaoyi" % "ammonite-repl" % "1.7.4" % "provided").cross(CrossVersion.full),
    "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "provided"
  ) ++ Seq("io.circe" %% "circe-core", "io.circe" %% "circe-generic", "io.circe" %% "circe-parser").map(_ % circeVersion)
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "sparkling.shade.@0").inLibrary(guava, failureaccess, webarchiveCommons).inProject)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}