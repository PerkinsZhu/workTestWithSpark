package spark

import org.apache.spark.sql.SparkSession
import org.junit.{After, Test}

/**
  * Created by PerkinsZhu on 2018/5/27 10:19
  **/
class BaseTest {
  System.setProperty("hadoop.home.dir", "E:\\Program Files\\hadoop-2.9.1")
  /*
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.test")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.test")
    val sc = new SparkContext(conf)
  */
  val spark = SparkSession
    .builder.master("local")
    .appName("WordCount")
    .getOrCreate()
  val sc = spark.sparkContext
  val rddData = sc.textFile("G:\\test\\test.txt")


  @After
  def stopSparkContext(): Unit = {
    if (!sc.isStopped) {
      sc.stop()
    }
  }

  @Test
  def testWordCount(): Unit = {
    rddData.flatMap(line => line.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _).foreach(println)
    Thread.sleep(100000)
  }

  @Test
  def testWordCount2(): Unit = {
    val splitData = rddData.flatMap(_.split("\\s+")).map(word => (word, 1))
    splitData.foreach(data => {
      println("----------------------")
      println(data._1.mkString("") + "--->" + data._2)
    })
    val result = splitData.reduceByKey((x, y) => {
      println(s"$x ---> $y")
      x + y
    })
    result.foreach(data => {
      println(data)
    })

  }


  @Test
  def testPi(): Unit = {
    val count = sc.parallelize(1 to 5).filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / 5}")
  }

  @Test
  def testBroadCast(): Unit ={
    val temp = sc.broadcast(Array(1,2,3,4,5))
    println(temp)
  }

}

