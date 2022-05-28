lazy val root = (project in file(".")).
  settings (
    name := "FinalProject",
    version := "1.0",
    scalaVersion := "2.13.8",
    ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation"),
    resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/",
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.6.19",
    libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.6.19",
    libraryDependencies += "com.typesafe.akka" %% "akka-serialization-jackson" % "2.6.19"
  )