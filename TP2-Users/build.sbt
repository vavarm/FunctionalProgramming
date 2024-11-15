ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

lazy val root = (project in file("."))
  .settings(
    name := "TP2-Users",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.neo4j" %% "neo4j-connector-apache-spark" % "5.3.1_for_spark_3"
    )
  )
