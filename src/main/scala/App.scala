package au.com.aeonsoftware
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Delta 101")
      .master("local")
      .getOrCreate()

    val df = spark.read.json("./data/*.json")

    df.show()

  }
}
