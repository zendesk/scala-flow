lazy val commonSettings = Seq(
  organization := "com.zendesk",
  version := "0.0.1",
  
  scalaVersion := "2.12.1",
  scalacOptions ++= Seq("-feature")
)

lazy val core = (project in file("core")).settings(commonSettings)

lazy val `scala-flow` = (project in file(".")).aggregate(core)
