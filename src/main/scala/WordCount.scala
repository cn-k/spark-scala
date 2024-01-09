import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration.Duration

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WalmartTransactionDataAnalyzer")
      .getOrCreate()

    import spark.implicits._

    val inputPath = "file:///C:/Users/cnk_a/Desktop/projects/java/spark-scala/source/*.csv"
//    val transactionData = spark.readStream.text(inputPath)
    val transactionData = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9000")
      .load()
    val words = transactionData.as[String].flatMap(_.split(" "))
    val wordsCount = words.groupBy("value").count()
    val query = wordsCount.writeStream.outputMode(OutputMode.Complete()).trigger(Trigger.ProcessingTime("3 seconds"))
      .format("console").start()
    query.awaitTermination()
  }
}
  