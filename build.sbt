import BuildHelper._

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.dev/zio-memberlist/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("pshemass", "Przemyslaw Wierzbicki", "rzbikson@gmail.com", url("https://github.com/pshemass"))
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("fix", "; all compile:scalafix test:scalafix; all scalafmtSbt scalafmtAll")
addCommandAlias("check", "; scalafmtSbtCheck; scalafmtCheckAll; compile:scalafix --check; test:scalafix --check")

addCommandAlias(
  "testJVM",
  ";zioMemberlistJVM/test"
)
addCommandAlias(
  "testJS",
  ";zioMemberlistJS/test"
)
addCommandAlias(
  "testNative",
  ";zioMemberlistNative/test:compile"
)

val zioVersion        = "1.0.11"
val zioNioVersion     = "1.0.0-RC11"
val zioLoggingVersion = "0.5.14"
val zioConfigVersion  = "1.0.10"

lazy val root = project
  .in(file("."))
  .settings(
    publish / skip := true
    //unusedCompileDependenciesFilter -= moduleFilter("org.scala-js", "scalajs-library")
  )
  .aggregate(
    zioMemberlistJVM,
    zioMemberlistJS,
    zioMemberlistNative,
    k8_experiment,
    docs
  )

lazy val zioMemberlist = crossProject(JSPlatform, JVMPlatform, NativePlatform)
  .in(file("zio-memberlist"))
  .settings(stdSettings("zio-memberlist"))
  .settings(crossProjectSettings)
  .settings(buildInfoSettings("zio.memberlist"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio"                %% "zio"                     % zioVersion,
      "dev.zio"                %% "zio-streams"             % zioVersion,
      ("dev.zio"               %% "zio-nio"                 % zioNioVersion).exclude("org.scala-lang.modules", "scala-collection-compat_2.13"),
      "dev.zio"                %% "zio-logging"             % zioLoggingVersion,
      "dev.zio"                %% "zio-config"              % zioConfigVersion,
      "com.lihaoyi"            %% "upickle"                 % "1.4.2",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.5.0",
      "dev.zio"                %% "izumi-reflect"           % "2.0.8",
      "dev.zio"                %% "zio"                     % zioVersion,
      "dev.zio"                %% "zio-test"                % zioVersion % Test,
      "dev.zio"                %% "zio-test-sbt"            % zioVersion % Test
    )
  )
  .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
  .enablePlugins(BuildInfoPlugin)

lazy val zioMemberlistJS = zioMemberlist.js
  .settings(jsSettings)
  .settings(libraryDependencies += "dev.zio" %%% "zio-test-sbt" % zioVersion % Test)
  .settings(scalaJSUseMainModuleInitializer := true)

lazy val zioMemberlistJVM = zioMemberlist.jvm
  .settings(dottySettings)
  .settings(libraryDependencies += "dev.zio" %%% "zio-test-sbt" % zioVersion % Test)
  .settings(scalaReflectTestSettings)

lazy val zioMemberlistNative = zioMemberlist.native
  .settings(nativeSettings)

lazy val k8_experiment = project
  .in(file("k8-experiment"))
  .settings(stdSettings("zio-memberlist-k8-experiment"))
  .settings(
    dockerBaseImage := "openjdk:11",
    dockerExposedPorts := Seq(5557),
    dockerUpdateLatest := false,
    dockerEntrypoint := Seq("bin/test-node"),
    dynverSeparator := "-"
  )
  .dependsOn(zioMemberlistJVM)
  .settings(
    fork := true,
    scalacOptions --= Seq("-Ywarn-dead-code", "-Wdead-code")
  )
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)

lazy val docs = project
  .in(file("zio-memberlist-docs"))
  .settings(
    moduleName := "zio-memberlist-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    crossScalaVersions := Seq(Scala212, Scala213, ScalaDotty),
    projectName := "ZIO Memberlist",
    mainModuleName := (zioMemberlistJVM / moduleName).value,
    projectStage := ProjectStage.Experimental,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(zioMemberlistJVM),
    docsPublishBranch := "master"
  )
  .dependsOn(zioMemberlistJVM)
  .enablePlugins(WebsitePlugin)
