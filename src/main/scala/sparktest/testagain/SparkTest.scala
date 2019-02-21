package sparktest.testagain

import org.apache.spark.sql.SparkSession

/**
  * Created by PerkinsZhu on 2019/1/21 15:22
  **/
object SparkTest {
  def main(args: Array[String]): Unit = {
    test01()
  }

  private def test01() = {
     val master = "spark://192.168.10.163:7077"
    val logFile = "G:\\test\\spark\\people.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master(master).getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
