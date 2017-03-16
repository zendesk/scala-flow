lazy val commonSettings = Seq(
  organization := "com.zendesk",
  version := "0.1.2",
  
  scalaVersion := "2.12.1",
  scalacOptions ++= Seq("-feature"),

  publishTo := Some("Artifactory" at sys.env.getOrElse("ARTIFACTORY_URL", "-")),
  credentials += Credentials(
    sys.env.getOrElse("ARTIFACTORY_REALM", "-"),
    sys.env.getOrElse("ARTIFACTORY_HOST", "-"),
    sys.env.getOrElse("ARTIFACTORY_USER", "-"),
    sys.env.getOrElse("ARTIFACTORY_KEY", "-")
  )
)

  // Publish POM rather than Ivy descriptor
  publishMavenStyle := true,
  // Don't include any other repositories - Maven Central requires all dependencies to also be in M.C.
  pomIncludeRepository := { _ => false },
  // Remove test scope dependencies from the POM
  pomPostProcess := { root =>
    import scala.xml.Node
    import scala.xml.transform.{RewriteRule, RuleTransformer}

    val rule = new RewriteRule {
      override def transform(node: Node): Seq[Node] = {
        if (node.label == "dependency" && (node \ "scope").text == "test") Seq() else node
      }
    }
    new RuleTransformer(rule).transform(root).head
  },
  // Extra information required by Maven Central
  pomExtra :=
    <url>https://github.com/zendesk/scala-flow</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git://github.com/zendesk/scala-flow.git</url>
      <connection>scm:git:git@github.com:zendesk/scala-flow.git</connection>
    </scm>
    <developers>
      <developer><id>bcasey-zd</id><name>Brendan Casey</name></developer>
      <developer><id>dasch</id><name>Daniel Schierbeck</name></developer>
    </developers>
)

lazy val `scala-flow` = (project in file("."))
  // Don't publish empty root project.
  .settings(commonSettings ++ Seq(publishArtifact := false))
  .aggregate(core)

lazy val core = (project in file("core")).settings(commonSettings)
