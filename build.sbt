name := "test-task"

version := "0.1"

scalaVersion := "2.12.0"

val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // logging
  "org.slf4j" % "slf4j-api" % "1.7.26",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  // cli
  "info.picocli" % "picocli" % "3.9.6",

  // testing
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}