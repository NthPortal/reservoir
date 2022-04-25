ThisBuild / scalaVersion    := "2.13.6"
ThisBuild / autoAPIMappings := true

// publishing info
inThisBuild(
  Seq(
    organization := "lgbt.princess",
    homepage     := Some(url("https://github.com/NthPortal/reservoir")),
    licenses     := Seq("The Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
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

// CI config
val PrimaryJava = "adopt@1.8"

inThisBuild(
  Seq(
    githubWorkflowTargetTags ++= Seq("v*"),
    githubWorkflowPublishTargetBranches ++= Seq(
      RefPredicate.StartsWith(Ref.Tag("v")),
    ),
    githubWorkflowJavaVersions := Seq(
      "adopt@1.8",
      "adopt@1.11",
      "adopt@1.15",
      "graalvm-ce-java8@20.2.0",
    ),
    githubWorkflowBuild := Seq(
      WorkflowStep.Sbt(List("compile", "test:compile"), name = Some("Compile")),
    ),
    githubWorkflowAddedJobs ++= Seq(
      WorkflowJob(
        "test",
        "Test",
        githubWorkflowJobSetup.value.toList ::: List(
          WorkflowStep.Sbt(List("test"), name = Some("Test")),
        ),
        javas = githubWorkflowJavaVersions.value.toList,
        scalas = crossScalaVersions.value.toList,
        needs = List("build"),
        matrixFailFast = Some(false),
      ),
      WorkflowJob(
        "bincompat",
        "Binary Compatibility",
        githubWorkflowJobSetup.value.toList ::: List(
          WorkflowStep.Sbt(List("clean", "mimaReportBinaryIssues"), name = Some("MiMa")),
        ),
        javas = List(PrimaryJava),
        scalas = crossScalaVersions.value.toList,
        needs = List("build"),
        matrixFailFast = Some(false),
      ),
      WorkflowJob(
        "coverage",
        "Coverage",
        githubWorkflowJobSetup.value.toList ::: List(
          WorkflowStep.Sbt(
            List("coverage", "test", "coverageAggregate", "coveralls"),
            name = Some("Coveralls"),
            cond = Some("env.COVERALLS_REPO_TOKEN != ''"),
          ),
        ),
        env = Map("COVERALLS_REPO_TOKEN" -> "${{ secrets.COVERALLS_REPO_TOKEN }}"),
        javas = List(PrimaryJava),
        scalas = crossScalaVersions.value.toList,
        needs = List("test"),
        matrixFailFast = Some(false),
      ),
      WorkflowJob(
        "formatting",
        "Formatting",
        githubWorkflowJobSetup.value.toList ::: List(
          WorkflowStep.Sbt(List("scalafmtCheckAll", "scalafmtSbtCheck"), name = Some("Formatting")),
        ),
        javas = List(PrimaryJava),
        scalas = List(scalaVersion.value),
        needs = List("build"),
        matrixFailFast = Some(false),
      ),
      WorkflowJob(
        "check-docs",
        "Check Docs",
        githubWorkflowJobSetup.value.toList ::: List(
          WorkflowStep.Sbt(List("doc"), name = Some("Check Scaladocs")),
        ),
        javas = List(PrimaryJava),
        scalas = List(scalaVersion.value),
        needs = List("build"),
        matrixFailFast = Some(false),
      ),
    ),
    githubWorkflowPublishPreamble += WorkflowStep.Use(UseRef.Public("olafurpg", "setup-gpg", "v3")),
    githubWorkflowPublish := Seq(
      WorkflowStep.Sbt(
        List("ci-release"),
        env = Map(
          "PGP_PASSPHRASE"    -> "${{ secrets.PGP_PASSPHRASE }}",
          "PGP_SECRET"        -> "${{ secrets.PGP_SECRET }}",
          "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
          "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}",
        ),
      ),
    ),
  ),
)

val sharedSettings = Seq(
  mimaPreviousArtifacts := Set().map(organization.value %% name.value % _),
  mimaFailOnNoPrevious  := true,
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.2.12" % Test,
  ),
  scalacOptions ++= Seq(
    "-Xlint",
    "-feature",
    "-Werror",
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

val akkaVersion = "2.6.18"
lazy val akka = project
  .in(file("akka-stream"))
  .settings(
    name := "reservoir-akka-stream",
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
  .dependsOn(
    core,
    akka,
  )
  .settings(
    name                  := "reservoir",
    mimaPreviousArtifacts := Set().map(organization.value %% name.value % _),
    mimaFailOnNoPrevious  := true,
    Compile / doc / sources :=
      (Compile / doc / sources).value ++
        (core / Compile / doc / sources).value ++
        (akka / Compile / doc / sources).value,
  )
