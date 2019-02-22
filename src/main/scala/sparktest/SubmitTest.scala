package sparktest

/**
  * Created by PerkinsZhu on 2018/8/18 15:01
  **/

import java.io.File

import com.mongodb.spark.MongoSpark
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import sparktest.sparksql.HiveTest.Record

object SubmitTest {


  /*  private val master = "spark://192.168.10.156:7077"
    private val remote_file = "hdfs://192.168.10.156:8020/test/input/employee.txt"*/
  private val master = "spark://192.168.10.163:7077"
  private val remote_file = "hdfs://192.168.10.163:9000/test/input/test.txt"
  val hdfs_input ="hdfs://192.168.10.163:9000/test/input/"
  val mongodbUri = "mongodb://192.168.10.192:27017/test.common-qa"
  val logger = Logger.getLogger(SubmitTest.getClass)
/*
  val sparkSession = SparkSession.builder()
    //    .config("spark.mongodb.input.uri", mongodbUri)
    .master(master)
    .appName("sparkTest").getOrCreate()
  val sparkContext = sparkSession.sparkContext;
*/
//val warehouseLocation = new File("/home/jinzhao/app/task/sparkWarehouse").getAbsolutePath
val warehouseLocation = "/user/hive/warehouse/"   // 这里有应该不用添加hadoop路径就好，直接写根目录路径
  val sparkSession = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext;

  import sparkSession.implicits._
  import sparkSession.sql
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

  def testSQL(): Unit = {
    val df = sparkSession.read.json(hdfs_input+"people.json")
    df.show()
  }


  def testHive(): Unit = {
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql(s"LOAD DATA LOCAL INPATH '/home/jinzhao/app/task/resources' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
    sql("SELECT COUNT(*) FROM src").show()
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()


    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = sparkSession.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
    // Save DataFrame to the Hive managed table
    val df = sparkSession.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM hive_records").show()

    // 这里的路径是基于hadoop的路径
    val dataDir = "/tmp/parquet_data"
    sparkSession.range(10).write.parquet(dataDir)
    // Create a Hive external Parquet table
    sql(s"CREATE EXTERNAL TABLE hive_ints(key int) STORED AS PARQUET LOCATION '$dataDir'")
    // The Hive external table should already have data
    sql("SELECT * FROM hive_ints").show()
    // +---+
    // |key|
    // +---+
    // |  0|
    // |  1|
    // |  2|
    // ...

    // Turn on flag for Hive Dynamic Partitioning·
    sparkSession.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sparkSession.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // Create a Hive partitioned table using DataFrame API
    df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
    // Partitioned column `key` will be moved to the end of the schema.
    sql("SELECT * FROM hive_part_tbl").show()


    sparkSession.stop()


  }

}

/**
  * 记录：
  * 自定义SQL聚合函数：http://spark.apache.org/docs/latest/sql-programming-guide.html#type-safe-user-defined-aggregate-functions
  * 读取json文件时，可以添加.option("multiLine"，true)的配置来读取多行的json文件
  *
  *
  */
