package mongodb

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.random

/**
  * Created by PerkinsZhu on 2018/6/22 17:28
  **/
object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\Program Files\\hadoop-2.9.1")
//    test01()
    test02
  }

  def test01(): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://perkins:perkins@127.0.0.1:27017/test.test")
      .config("spark.mongodb.output.uri", "mongodb://perkins:perkins@127.0.0.1:27017/test.test")
      .getOrCreate()
    val sc = spark.sparkContext
    import org.bson.Document
    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    MongoSpark.save(documents)

  }

  import scala.math.random
  def test02() {
    val spark = SparkSession
      .builder .master("local")
      .appName("Spark Pi")
      .getOrCreate()
    val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()
  }

}
