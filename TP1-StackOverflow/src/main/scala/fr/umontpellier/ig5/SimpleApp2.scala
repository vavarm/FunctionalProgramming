package fr.umontpellier.ig5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleApp2 {
  def main(args: Array[String]): Unit = {
    val logFile = "data/README.md"
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("spark")).count()
    val numBs = logData.filter(line => line.contains("scala")).count()
    println(s"Lines with 'spark': $numAs, Lines with 'scala': $numBs")
    spark.stop()
  }
}