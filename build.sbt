ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-scala"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2"
)