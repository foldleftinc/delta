package au.com.aeonsoftware

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType

import java.io.File

object TpccStreaming {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      throw new IllegalArgumentException("Please input arguments")
    }

    val DELTA_TABLE_ROOT_PATH = args(0)
    val KAFKA_BOOTSTRAP_SERVERS = args(1)

    //Clean up the delta store directory
    val file = new File(DELTA_TABLE_ROOT_PATH)
    if (file.exists()) FileUtils.deleteDirectory(file)

    //initialize Spark
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Delta Sample")
      .getOrCreate()


    import org.apache.spark.sql._
    import spark.implicits._


    val inputSchema = implicitly[Encoder[DebeziumEvent]].schema

    val inputDF = spark
//      .read
//      .text(SAMPLE_FILE_PATH)
      .readStream
      .format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", Configuration.topicNames)
      .option("startingOffsets", "earliest")
      .load()
    import org.apache.spark.sql._

    val extractedDF = inputDF
      .selectExpr("CAST(value AS String)")
      .select(functions.from_json($"value", inputSchema).as("data"))

    //extractedDF.printSchema()

    val beforeDF = extractedDF
      .select(col("data.payload.op"),
        col("data.payload.source.ts_ms"),
        col("data.payload.source.lsn"),
        col("data.payload.source.table"),
        col("data.payload.before") as 'row)
      .withColumn("image",lit("before"))
      .where("data.payload.before IS NOT NULL")

    val afterDF = extractedDF
      .select(col("data.payload.op"),
        col("data.payload.source.ts_ms"),
        col("data.payload.source.lsn"),
        col("data.payload.source.table"),
        col("data.payload.after"))
      .withColumn("image",lit("after"))
      .where("data.payload.after IS NOT NULL")

    val inputCustomerEncoder = implicitly[Encoder[Customer]]
    val customerSchema = inputCustomerEncoder.schema
    val inputOrdersEncoder = implicitly[Encoder[Orders]]
    val ordersSchema = inputOrdersEncoder.schema

    val customerDf = beforeDF.unionAll(afterDF)
      .select(functions.from_json($"row", customerSchema) as '_, $"op",$"ts_ms",$"lsn",$"image")
      .select($"_.*",$"op",$"ts_ms",$"lsn",$"image")
      .where("table = 'customer'")

    val orderDF = beforeDF.unionAll(afterDF)
      .select(functions.from_json($"row", ordersSchema) as '_, $"op",$"ts_ms",$"lsn",$"image")
      .select($"_.*",$"op",$"ts_ms",$"lsn",$"image")
      .where("table = 'orders'")

    val streamingPersonQuery = writeAsDelta(DELTA_TABLE_ROOT_PATH + "bronze-customer", customerDf)
    val streamingLocationQuery = writeAsDelta(DELTA_TABLE_ROOT_PATH + "bronze-orders", orderDF)

    streamingPersonQuery.awaitTermination()
    streamingLocationQuery.awaitTermination()
//    Configuration.TABLE_NAMES.foreach(n => {
//      println(s"Creating stream output for $n")
//    val n = "customer"
//      val query = beforeDF.unionAll(afterDF)
//        .select($"_.c_id",$"op",$"ts_ms",$"lsn",$"image")
//        .where(s"table = '$n'")
//      val stream = writeAsDelta(DELTA_TABLE_ROOT_PATH + s"bronze-$n", query)
//      stream.awaitTermination()
//    })

  }

  private def writeAsDelta[T](deltaTableName: String, df: DataFrame) = {
    val query = df
      .writeStream
      .trigger(Trigger.ProcessingTime("120 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        println(s"Writing to Delta $batchID")
        batchDF.write
          .format("delta")
          .mode("append")
          .save(deltaTableName)
      }
      .outputMode("update")
      .start()
    query
  }
}
