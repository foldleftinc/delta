package au.com.aeonsoftware

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.io.File

object App {
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

    val inputEncoder = implicitly[Encoder[DebeziumEvent]]
    val inputSchema = inputEncoder.schema

    val inputDF = spark
//      .read
//      .text(SAMPLE_FILE_PATH)
      .readStream
      .format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", "io.aeon.pg.pg-delta.public.person,io.aeon.pg.pg-delta.public.location")
      .option("startingOffsets", "earliest")
      .load()
    import org.apache.spark.sql._


    val extractedDF = inputDF
      .selectExpr("CAST(value AS STRING)")
      .select(functions.from_json($"value", inputSchema).as("data"))

    //TODO: DRY fix
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

    val personDF = beforeDF.unionAll(afterDF)
      .where("table = 'person'")
      .select($"row.id",$"row.locationid",$"row.name",$"op",$"ts_ms",$"lsn",$"image")

    val locationDF = beforeDF.unionAll(afterDF)
      .where("table = 'location'")
      .select($"row.id",$"row.name",$"op",$"ts_ms",$"lsn",$"image")

    val streamingPersonQuery = writeAsDelta(DELTA_TABLE_ROOT_PATH + "bronze-person", personDF)
    val streamingLocationQuery = writeAsDelta(DELTA_TABLE_ROOT_PATH + "bronze-location", locationDF)

    streamingPersonQuery.awaitTermination()
    streamingLocationQuery.awaitTermination()

  }

  private def writeAsDelta(deltaTableName: String, personDF: DataFrame) = {
    val query = personDF
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
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
