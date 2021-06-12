package au.com.aeonsoftware

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.streaming.Trigger

object App {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      //      throw new IllegalArgumentException("Please input ")
    }
    val SAMPLE_FILE_PATH = args(0)
    val DELTA_TABLE_ROOT_PATH = args(1)
    val KAFKA_BOOTSTRAP_SERVERS = args(2)

    //val DELTA_TABLE_NAME = "./data/delta-store/music-table"

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
      .option("subscribe", "io.aeon.pg.pg-delta.public.person")
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
        col("data.payload.before") as 'person)
      .withColumn("image",lit("before"))
      .where("data.payload.before IS NOT NULL")

    val afterDF = extractedDF
      .select(col("data.payload.op"),
        col("data.payload.source.ts_ms"),
        col("data.payload.source.lsn"),
        col("data.payload.after"))
      .withColumn("image",lit("after"))
      .where("data.payload.after IS NOT NULL")

    val personDF = beforeDF.unionAll(afterDF)
      .select($"person.id",$"person.locationid",$"person.name",$"op",$"ts_ms",$"lsn",$"image")

    val query = personDF
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        println(s"Writing to Delta $batchID")
        batchDF.write
          .format("delta")
          .mode("append")
          .save(DELTA_TABLE_ROOT_PATH + "bronze-person")
      }
      .outputMode("update")
      .start()

    query.awaitTermination()

  }
}
