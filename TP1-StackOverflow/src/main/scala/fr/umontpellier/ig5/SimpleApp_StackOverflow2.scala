package fr.umontpellier.ig5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SimpleApp_StackOverflow2 extends App {
  val programStartTime = System.nanoTime()

  Logger.getLogger("org").setLevel(Level.ERROR)

  val csvDataFile = "data/stackoverflow.csv"

  val spark = SparkSession.builder
    .appName("Stackoverflow Application")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val schema = new StructType()
    .add("postTypeId", IntegerType, nullable = true)
    .add("id", IntegerType, nullable = true)
    .add("acceptedAnswer", StringType, nullable = true)
    .add("parentId", IntegerType, nullable = true)
    .add("score", IntegerType, nullable = true)
    .add("tag", StringType, nullable = true)

  val df = spark.read
    .option("header", "false")
    .schema(schema)
    .csv(csvDataFile)
    .drop("acceptedAnswer")

  println(s"\nCount of records in CSV file: ${df.count()}")
  df.printSchema()
  df.show(5)

  df.createOrReplaceTempView("stackoverflow")

  val top5Scores = spark.sql("SELECT id, score FROM stackoverflow ORDER BY score DESC LIMIT 5")
  println("\nTop 5 scores:")
  top5Scores.show()

  val top5ScoresWithTags = spark.sql("SELECT id, score, tag FROM stackoverflow WHERE tag IS NOT NULL ORDER BY score DESC LIMIT 5")
  println("\nTop 5 scores with tags:")
  top5ScoresWithTags.show()

  val popularTags = spark.sql("SELECT tag, COUNT(*) AS frequency FROM stackoverflow WHERE tag IS NOT NULL GROUP BY tag ORDER BY frequency DESC LIMIT 5")
  println("\nPopular tags:")
  popularTags.show()

  val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
  println(s"\nProgram execution time: $programElapsedTime seconds")
  spark.stop()

}