ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "TP1-StackOverflow",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.neo4j" %% "neo4j-connector-apache-spark" % "5.3.1_for_spark_3"
    )
  )
