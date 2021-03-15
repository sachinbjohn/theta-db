name := "VWAP"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++=Seq(
  "ch.epfl.data" % "dbtoaster-sstore_2.11" % "2.3",
  "ch.epfl.data" % "dbtoaster-core_2.11" % "2.3",
  "org.scalatest" %% "scalatest" % "3.2.5" % "test"
)
