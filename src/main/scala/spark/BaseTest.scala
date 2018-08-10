package spark

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
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


  @Test
  def testParallelize(): Unit = {
    val data = Seq(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data, 5)
    val result = distData.map(Math.pow(_, 10)).fold(0)(_ + _)
    println(result)
  }

  import spark.implicits._

  @Test
  def testSparkSql(): Unit = {
    val df = spark.read.json("G:\\test\\spark\\person.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 20).show()
    df.groupBy("age").count().show()
    println("==========================================================")

    df.createOrReplaceTempView("people")
    val sql = spark.sql("SELECT * FROM people")
    sql.show()

    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()

  }


  @Test
  def testBean(): Unit = {
    val ds = Seq(Person("jack", 10), Person("tom", 100)).toDS()
    ds.show()

    println((1 to 100) toDS() map (_ * 10) collect() mkString ("、"))

    spark.read.json("G:\\test\\spark\\person.json").as[Person].show()

    println("======自定义schema===========")
    //自定义schema
    val rdd = sc.textFile("G:\\test\\spark\\people.txt").map(_.split(",")).map(attr => Row(attr(0), attr(1).trim))
    val fields = "name age".split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val df = spark.createDataFrame(rdd, schema)
    df.createOrReplaceTempView("people")
    val data = spark.sql("SELECT * FROM people")
    data.show()
    //对结果进行转换
    data.map(item => "name：" + item(0) + "\t age:" + item(1)).show()


  }

  @Test
  def testAggregations(): Unit = {
    spark.udf.register("myAverage", MyAverage)
    val df = spark.read.json("G:\\test\\spark\\employees.json")
    df.createOrReplaceTempView("employees")
    df.show()
    //把salary 列传入聚合器中计算
    spark.sql("SELECT myAverage(salary) as average_salary FROM employees").show()
  }

  @Test
  def testLoadAndSave(): Unit = {
    val df = spark.read.load("G:\\test\\spark\\users.parquet")
    df.show()
    val data = df.select("name", "favorite_color")
    data.show()
    data.write.mode(SaveMode.Overwrite).save("namesAndFavColors.parquet")

    val csv = spark.read.format("csv").option("seq", ";")
      .option("inferSchema", "true").option("header", "true")
      .load("G:\\test\\spark\\people.csv")
    csv.show()
  }

  @Test
  def testSelectFromFile(): Unit ={
    val df = spark.sql("SELECT * FROM parquet.`G:\\test\\spark\\users.parquet`")
    df.show()
  }


}

object MyAverage extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!buffer.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}

case class Person(name: String, age: Long)