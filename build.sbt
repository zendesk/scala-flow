lazy val commonSettings = Seq(
  organization := "com.zendesk",
  version := "0.1.0-SNAPSHOT",
  
  scalaVersion := "2.12.1",
  scalacOptions ++= Seq("-feature"),

  publishTo := Some("Artifactory" at sys.env.getOrElse("ARTIFACTORY_URL", "-") + new java.util.Date().getTime),
  credentials += Credentials(
    sys.env.getOrElse("ARTIFACTORY_REALM", "-"),
    sys.env.getOrElse("ARTIFACTORY_HOST", "-"),
    sys.env.getOrElse("ARTIFACTORY_USER", "-"),
    sys.env.getOrElse("ARTIFACTORY_KEY", "-")
  )
)

lazy val core = (project in file("core")).settings(commonSettings)

lazy val `scala-flow` = (project in file(".")).settings(commonSettings).aggregate(core)

