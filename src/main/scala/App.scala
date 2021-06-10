package au.com.aeonsoftware
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import java.io.File


object App {
  def main(args: Array[String]): Unit = {

    val DELTA_TABLE_NAME = "./data/delta-store/music-table"

    val spark = SparkSession
      .builder()
      .appName("Delta 101")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val musicReviewSchema = new StructType()
      .add("reviewerID", "string")
      .add("asin", "string")
      .add("reviewerName", "string")

    val data = spark
      .read
      .schema(musicReviewSchema)
      .json("./data/*.json")

    val file = new File(DELTA_TABLE_NAME)
    if (file.exists()) FileUtils.deleteDirectory(file)

    data
      .write
      .format("delta")
      .save(DELTA_TABLE_NAME)

  }
}
