name := "scala-flow-core"

enablePlugins(BoilerplatePlugin)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,

  "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "1.9.0",

  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.1" % Test,
  "org.hamcrest" % "hamcrest-library" % "1.3" % Test,
  "junit" % "junit" % "4.12" % Test
)
