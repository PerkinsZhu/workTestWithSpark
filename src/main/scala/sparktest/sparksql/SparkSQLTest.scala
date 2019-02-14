package sparktest.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by PerkinsZhu on 2019/2/14 10:29
  **/
object SparkSQLTest {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  def testBase(): Unit = {
    val df = spark.read.json("examples/src/main/resources/people.json")
    df.show()
  }

  def main(args: Array[String]): Unit = {
    testBase()
  }

}
