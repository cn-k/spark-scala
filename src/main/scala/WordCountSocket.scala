import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.concurrent.duration.Duration

object WordCountSocket {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WalmartTransactionDataAnalyzer")
      .config("spark.sql.shuffle.partitions",3)
      .getOrCreate()

    import spark.implicits._

    val inputPath = "file:///C:/Users/cnk_a/Desktop/projects/java/spark-scala/source/*.csv"
//    val transactionData = spark.readStream.text(inputPath)
    val transactionData = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9000")
      .load()
    val words = transactionData.as[String].flatMap(_.split(" ")).map(word => (word, new Timestamp(System
      .currentTimeMillis()).toString))
    val wordsCount = words.groupBy(window($"_2", "10 seconds"), $"_1").count()
    val query = wordsCount.writeStream
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .option("truncate", false)
      .format("console").start()
    query.awaitTermination()
  }
}
  