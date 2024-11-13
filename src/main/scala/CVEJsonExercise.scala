package fr.umontpellier.ig5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import java.io.File

object CVEJsonExercise {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("CVE Extractor Application")
      .master("local[*]")
      .getOrCreate()

    // Define the input and output directories
    val inputDir = "data" // Directory containing the CVE JSON files
    val outputFile = "output/extracted_cve_data.json"

    // List all files in the input directory
    val jsonFiles = new File(inputDir).listFiles
      .filter(file => file.isFile && file.getName.endsWith(".json"))
      .filter(file => {
        val year = file.getName.takeRight(9).take(4).toInt
        year >= 2002 && year <= 2024
      })
      .map(_.getPath)

    // Process each JSON file
    val cveDataFrames = jsonFiles.map { filePath =>
      val jsonData = spark.read.option("multiline", "true").json(filePath)

      // Extract the relevant fields for each CVE entry
      jsonData
        .select(explode(col("CVE_Items")).as("item"))
        .select(
          col("item.cve.CVE_data_meta.ID").as("ID"),
          col("item.cve.description.description_data")(0)("value").as("Description"),
          col("item.impact.baseMetricV3.cvssV3.baseScore").as("baseScore"),
          col("item.impact.baseMetricV3.cvssV3.baseSeverity").as("baseSeverity"),
          col("item.impact.baseMetricV3.exploitabilityScore").as("exploitabilityScore"),
          col("item.impact.baseMetricV3.impactScore").as("impactScore")
        )
    }

    // Combine all data into a single DataFrame
    val combinedData: DataFrame = cveDataFrames.reduce(_ union _)

    // Write the combined data to the output JSON file
    combinedData.write
      .mode("overwrite")
      .json(outputFile)

    println(s"Extracted data has been written to $outputFile")

    // Stop the Spark session
    spark.stop()
  }
}
