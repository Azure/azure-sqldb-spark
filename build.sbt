val sparkVersion = "2.4.5"
lazy val root = project.in(file("."))
  .settings(
    crossScalaVersions := Seq("2.12.10", "2.11.12"),
    scalaVersion := crossScalaVersions.value.head,
    organization := "com.microsoft.azure",
    name := "azure-sqldb-spark",
    version := "1.0.3",
    developers := List(Developer("Azure SQL DB Devs", "Microsoft", email = "opensource@microsoft.com", url = url("http://www.microsoft.com"))),
    libraryDependencies := Seq(
      "junit" % "junit" % "4.8.1" % "test",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.scalactic" %% "scalactic" % "3.0.4",
      "org.scalatest" %% "scalatest" % "3.0.4" % "test",
      "com.microsoft.azure" % "adal4j" % "1.2.0",
      "com.microsoft.sqlserver" % "mssql-jdbc" % "6.4.0.jre8",
    ),
    scalacOptions ++= Seq("-deprecation", "-feature"),
    scalastyleConfig := file("lib/scalastyle_config.xml"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last == "git.properties" => MergeStrategy.discard
      case PathList("org", xs @ _*) => MergeStrategy.first
      case PathList("com", xs @ _*) => MergeStrategy.first
      case PathList("javax", xs @ _*) => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )
