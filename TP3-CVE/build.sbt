ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "TP3-CVE",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.neo4j" %% "neo4j-connector-apache-spark" % "5.3.1_for_spark_3",
      "org.mongodb.scala" %% "mongo-scala-driver" % "5.2.0"
    )
  )
