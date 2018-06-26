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
  val readStream = spark.readStream
  val sc = spark.sparkContext
  val rddData = sc.textFile("G:\\test\\application-2018-06-15")
  sc.setLogLevel("WARN")

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
    println(rddData.partitions.size)
    Thread.sleep(10000)
    val result = rddData.flatMap(_.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _).count()
    println(result)
    //    result.foreach(data => {
    //      println(data)
    //    })

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
  def testBroadCast(): Unit = {
    val temp = sc.broadcast(Array(1, 2, 3, 4, 5))
    println(temp)
  }



  import spark.implicits._

  @Test
  def testStream(): Unit = {
    val lines = readStream.format("socket")
      .option("host", "192.168.10.192")
      .option("port", 9999)
      .load()
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()
    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}

