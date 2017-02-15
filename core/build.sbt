name := "scala-flow-core"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,

  "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "1.9.0",
  "com.google.cloud.bigtable" % "bigtable-hbase-dataflow" % "0.9.5",
  "io.netty" % "netty-tcnative-boringssl-static" % "1.1.33.Fork19",

  "org.slf4j" % "slf4j-jdk14" % "1.7.7" % Runtime,

  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.hamcrest" % "hamcrest-library" % "1.3" % Test,
  "junit" % "junit" % "4.12" % Test
)
