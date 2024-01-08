import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.concurrent.duration._

object WalmartTransactionDataAnalyzer {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "")
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WalmartTransactionDataAnalyzer")
      .getOrCreate()
    import spark.implicits._

    val schema = StructType(
      Array(
        StructField("Date", StringType, nullable = true),
        StructField("Store", IntegerType, nullable = true),
        StructField("ProductID", IntegerType, nullable = true),
        StructField("Quantity", IntegerType, nullable = true),
        StructField("UnitPrice", IntegerType, nullable = true),
        StructField("TotalPrice", IntegerType, nullable = true)
      )
    )

    // Read stream from a directory as if new files were being added continuously
    val transactionData = spark.readStream
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load("file:///C:/Users/cnk_a/Desktop/projects/java/spark-scala/source/*.csv")

    val windowedCounts = transactionData
      .groupBy(window($"timestamp", "10 seconds"), $"productID")
      .agg(sum("TotalPrice").as("revenue"))

    // You may further process windowedCounts as needed for analysis

    val query = windowedCounts.writeStream
      .outputMode("complete") // Specify the output mode as needed (e.g., complete, append, update)
      .format("console") // Output to console for example purposes
      .trigger(Trigger.ProcessingTime(5.minutes)) // Trigger interval
      .start()

    query.awaitTermination() // Keep the streaming query running indefinitely
  }
}
