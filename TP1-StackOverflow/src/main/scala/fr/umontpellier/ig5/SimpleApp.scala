package fr.umontpellier.ig5

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "data/README.md"
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}