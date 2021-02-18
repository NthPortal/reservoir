ThisBuild / scalaVersion := "2.13.4"
ThisBuild / autoAPIMappings := true

// publishing info
inThisBuild(
  Seq(
    organization := "lgbt.princess",
    homepage := Some(url("https://github.com/NthPortal/reservoir")),
    licenses := Seq("The Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    developers := List(
      Developer(
        "NthPortal",
        "April | Princess",
        "dev@princess.lgbt",
        url("https://nthportal.com"),
      ),
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/NthPortal/reservoir"),
        "scm:git:git@github.com:NthPortal/reservoir.git",
        "scm:git:git@github.com:NthPortal/reservoir.git",
      ),
    ),
  ),
)

val sharedSettings = Seq(
  mimaPreviousArtifacts := Set().map(organization.value %% name.value % _),
  mimaFailOnNoPrevious := true,
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.2.4" % Test,
  ),
  scalacOptions ++= {
    if (isSnapshot.value) Nil
    else
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 13)) => Seq("-opt:l:inline", "-opt-inline-from:lgbt.princess.reservoir.**")
        case _             => Nil
      }
  },
  autoAPIMappings := true,
)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "reservoir-core",
  )
  .settings(sharedSettings)

val akkaVersion = "2.6.12"
lazy val akka = project
  .in(file("akka"))
  .settings(
    name := "reservoir-akka",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    ),
  )
  .settings(sharedSettings)
  .dependsOn(core)

lazy val root = project
  .in(file("."))
  .aggregate(
    core,
    akka,
  )
  .settings(
    name := "reservoir",
    skip in publish := true,
    mimaFailOnNoPrevious := false,
  )
