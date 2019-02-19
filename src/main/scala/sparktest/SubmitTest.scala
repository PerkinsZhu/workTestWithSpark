package sparktest

/**
  * Created by PerkinsZhu on 2018/8/18 15:01
  **/

import com.mongodb.spark.MongoSpark
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SubmitTest {
  private val master = "spark://192.168.10.156:7337"
  private val remote_file = "hdfs://192.168.10.156:9000/test/input/employee.txt"
  val mongodbUri = "mongodb://192.168.10.192:27017/test.common-qa"
  val logger = Logger.getLogger(SubmitTest.getClass)
  val sparkSession = SparkSession.builder()
//    .config("spark.mongodb.input.uri", mongodbUri)
    .master(master)
    .appName("sparkTest").getOrCreate()
  val sparkContext = sparkSession.sparkContext;

  import sparkSession.implicits._

  def testSpark(): Unit = {
    val textFile = sparkContext.textFile(remote_file)
    textFile.map(_.split(" ")).take(1000).foreach(item => {
      logger.info(item.mkString("、"))
      println(item.mkString("、"))
    })
  }

  def testSubmit() = {
    val textFile = sparkContext.textFile(remote_file)
    textFile.take(1000).foreach(item => {
      logger.info(item)
      println(item)
    })
  }

  def testCount() = {
    sparkContext.textFile(remote_file).flatMap(line => line.split("\t")).map((_, 1)).reduceByKey((_ + _)).collect().foreach(println)
  }

  case class Person(name: String, age: Int)

  def testDataSetStream(): Unit = {
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val lines = sparkSession.readStream.format("socket")
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
    val ssc = new StreamingContext(sparkContext, Seconds(1))
    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    lines.flatMap(_.split(";")).map((_, 1)).reduceByKey(_ + _).foreachRDD((k, v) => println(k + "--->" + v))
    ssc.start()
    ssc.awaitTermination()
  }

  def testMongod(): Unit = {
    val rdd = MongoSpark.load(sparkContext)
    println(rdd.count)
    println(rdd.first.toJson)

  }

}

/**
  * 记录：
  * 自定义SQL聚合函数：http://spark.apache.org/docs/latest/sql-programming-guide.html#type-safe-user-defined-aggregate-functions
  * 读取json文件时，可以添加.option("multiLine"，true)的配置来读取多行的json文件
  *
  *
  */
