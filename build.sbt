import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.github.io/zio-memberlist/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("pshemass", "Przemyslaw Wierzbicki", "rzbikson@gmail.com", url("https://github.com/pshemass"))
    ),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/zio-memberlist/"), "scm:git:git@github.com:zio/zio-memberlist.git")
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion        = "1.0.0"
val zioNioVersion     = "1.0.0-RC9"
val zioLoggingVersion = "0.4.0"
val zioConfigVersion  = "1.0.0-RC26"

lazy val root = project
  .in(file("."))
  .settings(skip in publish := true)
  .aggregate(memberlist)

lazy val memberlist =
  project
    .in(file("memberlist"))
    .settings(stdSettings("zio-memberlist"))
    .settings(buildInfoSettings("zio.memberlist"))
    .settings(
      libraryDependencies ++= Seq(
        "dev.zio"                %% "zio"                     % zioVersion,
        "dev.zio"                %% "zio-streams"             % zioVersion,
        "dev.zio"                %% "zio-nio"                 % zioNioVersion,
        "dev.zio"                %% "zio-logging"             % zioLoggingVersion,
        "dev.zio"                %% "zio-config"              % zioConfigVersion,
        "com.lihaoyi"            %% "upickle"                 % "1.2.0",
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6",
        "dev.zio"                %% "zio-test"                % zioVersion % Test,
        "dev.zio"                %% "zio-test-sbt"            % zioVersion % Test,
        ("com.github.ghik" % "silencer-lib" % "1.6.0" % Provided).cross(CrossVersion.full),
        compilerPlugin(("com.github.ghik" % "silencer-plugin" % "1.6.0").cross(CrossVersion.full))
      )
    )
    .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))

lazy val docs = project
  .in(file("zio-docs"))
  .settings(
    skip.in(publish) := true,
    moduleName := "zio-memberlist-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion
    ),
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(root),
    target in (ScalaUnidoc, unidoc) := (baseDirectory in LocalRootProject).value / "website" / "static" / "api",
    cleanFiles += (target in (ScalaUnidoc, unidoc)).value,
    docusaurusCreateSite := docusaurusCreateSite.dependsOn(unidoc in Compile).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(unidoc in Compile).value
  )
  .dependsOn(root)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
