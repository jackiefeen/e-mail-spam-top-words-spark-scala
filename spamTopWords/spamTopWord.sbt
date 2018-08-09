// spamTopWord.sbt file

name := "spamTopWords"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"
)
