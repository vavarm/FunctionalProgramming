package fr.umontpellier.polytech.ig5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

object ETL extends App {
  val programStartTime = System.nanoTime()

  // Set logging level to reduce unnecessary logs
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Define the years to process
  val years = (2002 to 2024).toList

  // Initialize Spark session
  val spark = SparkSession.builder
    .appName("CVE ETL Application")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Define a function to process a single JSON file and extract the transformed data
  def processJsonFile(filePath: String): DataFrame = {
    val df = spark.read.option("multiline", "true").json(filePath)

    // Explode the CVE_Items array and extract relevant fields
    df.select(F.explode(F.col("CVE_Items")).alias("cve_item"))
      .select(
        F.col("cve_item.cve.CVE_data_meta.ID").alias("ID"),
        F.expr("cve_item.cve.description.description_data[0].value").alias("Description"),
        F.col("cve_item.impact.baseMetricV3.cvssV3.baseScore").alias("baseScore"),
        F.col("cve_item.impact.baseMetricV3.cvssV3.baseSeverity").alias("baseSeverity"),
        F.col("cve_item.impact.baseMetricV3.exploitabilityScore").alias("exploitabilityScore"),
        F.col("cve_item.impact.baseMetricV3.impactScore").alias("impactScore")
      )
  }

  // Process all JSON files and combine the results into a single DataFrame
  val allTransformedData: DataFrame = years
    .map(year => s"data-in/nvdcve-1.1-$year.json") // Generate file paths
    .map(processJsonFile)                         // Process each file
    .reduce(_ union _)                            // Combine all DataFrames into one

  // Print the schema and sample data for verification
  println("Combined Transformed Data Schema:")
  allTransformedData.printSchema()

  println("Sample Combined Transformed Data:")
  allTransformedData.show(5, truncate = false)

  // Save the combined data to a single JSON file
  val outputFile = "data-out/transformed_data.json"
  allTransformedData
    .coalesce(1) // Combine into a single partition for a single output file
    .write
    .mode("overwrite")
    .json(outputFile)

  val programEndTime = System.nanoTime()
  println(s"Program executed in ${(programEndTime - programStartTime) / 1e9} seconds")
}