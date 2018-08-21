package sparktest

/**
  * Created by PerkinsZhu on 2018/8/18 15:01
  **/

import java.io.File

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import sparktest.WordCount.sparkSession

object WordCount {
  private val master = "spark://192.168.10.156:7077"
  private val remote_file = "hdfs://192.168.10.156:9000/test/input/test.txt"
  val logger = Logger.getLogger(WordCount.getClass)
  private val dir = System.getProperty("user.dir")
  lazy private val sc = new SparkContext(new SparkConf())

  def testSpark(): Unit = {
    println("---startTask--")

    //这些配置就相当于在程序中执行submit操作,运行时可以直接通过java -jar来运行
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster(master)
      .set("spark.executor.memory", "900m")
      .setJars(List(dir + File.separator + "workTestWithSpark.jar"))

    val sc = new SparkContext(conf)
    val textFile = sc.textFile(remote_file)
    textFile.take(1000).foreach(item => {
      logger.info(item)
      println(item)
    })
    println("task is over")
  }

  def testSubmit() = {
    println("---submit test--")
    //如果使用submit命令，则这里可以取消sparktest.WordCount.testSpark中的配置
    val textFile = sc.textFile(remote_file)
    textFile.take(1000).foreach(item => {
      logger.info(item)
      println(item)
    })
    println("task is over")
  }

  def testCount() = {
    sc.textFile(remote_file).flatMap(line => line.split("\t")).map((_, 1)).reduceByKey((_ + _)).collect().foreach(println)
  }

  case class Person(name: String, age: Int)

  def testDataSetStream(): Unit = {
    val spark = SparkSession.builder().appName("netWorkWordCount").getOrCreate()
    import spark.implicits._
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val lines = spark.readStream.format("socket")
      .option("host", "192.168.10.156")
      .option("port", 9999)
      .load()
    val person = lines.as[String].map(line => {
      val array = line.split(" ")
      Person(array(0), Integer.parseInt(array(1)))
    })

    //注意这里 groupBy的参数"_1"，这里因为被map为了Tpule,所以需要使用_1来进行group
    val ds = person.filter(_.age > 10).map(item => (item.name, 1)).groupBy("_1").count()
    val query = ds.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }

  def testNetWorkWordCount(): Unit = {
    val words: Dataset[String] = getWords
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }

  val sparkSession = SparkSession.builder().appName("netWorkWordCount").getOrCreate()
  //注意这里导入的不是包名,而是spark对象的一个属性
  import sparkSession.implicits._

  private def getWords = {
    val lines = sparkSession.readStream.format("socket")
      .option("host", "192.168.10.156")
      .option("port", 9999).load()

    val words = lines.as[String].flatMap(line => {
      line.split(" ")
    })
    words
  }

  def testWater() = {
    val words: Dataset[String] = getWords
    /*val windowedCounts = words
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word")
      .count()*/
  }

  def testDStream() = {
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(1))
    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    lines.flatMap(_.split(";")).map((_, 1)).reduceByKey(_ + _).foreachRDD((k, v) => println(k + "--->" + v))
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]) {
    logger.info("-----task start-----")
    //    testSpark()
    //    testSubmit()
    //    testCount()
    //    testNetWorkWordCount()
    //    testDataSetStream()
    //    testWater()
    testDStream()
    logger.info("-----task over-----")
  }
}