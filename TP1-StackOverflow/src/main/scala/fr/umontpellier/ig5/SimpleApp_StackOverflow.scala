package fr.umontpellier.ig5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleApp_StackOverflow extends App {
  val programStartTime = System.nanoTime()

  Logger.getLogger("org").setLevel(Level.ERROR)

  val csvDataFile = "data/stackoverflow.csv"

  val spark = SparkSession.builder
    .appName("Stackoverflow Application")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .csv(csvDataFile)

  println(s"\nCount of records in CSV file: ${df.count()}")
  df.printSchema()
  df.show(5)


  val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
  println(s"\nProgram execution time: $programElapsedTime seconds")
  spark.stop()

}