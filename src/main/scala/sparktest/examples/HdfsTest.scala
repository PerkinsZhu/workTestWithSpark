package sparktest.examples

import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.io.Source

/**
  * Created by PerkinsZhu on 2018/8/23 13:20
  **/
class HdfsTest {
  System.setProperty("HADOOP_USER_NAME", "jinzhao")
  val sparkSession = SparkSession.builder().master("local")
    .appName("hdfsTest")
    .getOrCreate()


  @Test
  def testShow(): Unit = {
    val fileRdd = sparkSession.read.textFile("hdfs://192.168.10.156:9000/test/input/test.txt").rdd
    fileRdd.foreach(println _)
    sparkSession.stop()
  }

  @Test
  def writeToFile(): Unit = {
    val outDirect = "hdfs://192.168.10.156:9000/test/hdfstest/out/test.txt"
    val source = Source.fromFile("G:\\test\\log.txt").getLines().toList
    sparkSession.sparkContext.parallelize(source).saveAsTextFile(outDirect)
    sparkSession.sparkContext.textFile(outDirect).flatMap(_.split("\\s+|\\t")).filter(_.nonEmpty).map((_, 1))
      .countByKey().foreach(item => println(item._1 + "--->" + item._2))
    sparkSession.stop()
  }


}
