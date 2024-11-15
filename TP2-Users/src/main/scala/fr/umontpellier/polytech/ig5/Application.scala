package fr.umontpellier.polytech.ig5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Application extends App {

  val programStartTime = System.nanoTime()

  Logger.getLogger("org").setLevel(Level.ERROR)

  val csvDataFile = "data/users.csv"

  val spark = SparkSession.builder
    .appName("Stackoverflow Application")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val schema = new StructType()
    .add("id", IntegerType, nullable = true)
    .add("name", StringType, nullable = true)
    .add("age", IntegerType, nullable = true)
    .add("city", StringType, nullable = true)

  val df = spark.read
    .option("header", "true")
    .schema(schema)
    .csv(csvDataFile)

  println(s"\nCount of records in CSV file: ${df.count()}")
  df.printSchema()
  df.show(5)

  df.createOrReplaceTempView("users")

  // Filter users aged 25 and above

  val users25AndAbove = spark.sql("SELECT * FROM users WHERE age >= 25")
  println(s"Users aged 25 and above: ${users25AndAbove.count()}")
  users25AndAbove.show()

  // Transform data to extract names and cities

  val namesAndCities = spark.sql("SELECT name, city FROM users")
  namesAndCities.show()

  // Group users by city

  val usersByCity = spark.sql("SELECT city, count(*) as user_count FROM users GROUP BY city")
  usersByCity.show()

  val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
  println(s"\nProgram execution time: $programElapsedTime seconds")
  spark.stop()
}
