package sparktest

import com.mongodb.spark.MongoSpark
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import sparktest.SubmitTest.Person
import sparktest.examples.SparkContextTest.Student

/**
  * Created by PerkinsZhu on 2018/8/22 10:07
  **/
object LocalTest {

  private val master = "spark://192.168.10.163:7077"
  private val remote_file = "hdfs://192.168.10.163:9000/test/input/test.txt"
  val logger = Logger.getLogger(LocalTest.getClass)

  def testSpark(): Unit = {
    println("---startTask--")
    //这些配置就相当于在程序中执行submit操作,运行时可以直接通过java -jar来运行
    val conf = new SparkConf()
      .setAppName("sparkTest")
      .setMaster(master)
      .set("spark.executor.memory", "900m")
      .setJars(List("F:\\myCode\\workTestWithSpark\\classes\\artifacts\\workTestWithSpark_jar\\workTestWithSpark.jar"))
    //      .setJars(new File("F:\\myCode\\workTestWithSpark\\classes\\artifacts\\workTestWithSpark_jar").listFiles().map(_.getAbsolutePath))

    val sc = new SparkContext(conf)
    val textFile = sc.textFile(remote_file)
    textFile.map(_.split(" ")).take(1000)
    println("task is over")
  }

  def main(args: Array[String]): Unit = {
    import org.bson.Document
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://192.168.10.192:27017/test.copyqa")
      .config("spark.mongodb.input.readPreference.name", "secondaryPreferred")
      .config("spark.mongodb.output.uri", "mongodb://192.168.10.192:27017/test.copyout")
      .config("spark.mongodb.input.database", "test")
      .getOrCreate()
    val rdd = MongoSpark.load(spark.sparkContext).toDF()
    println(rdd.count)
    println(rdd.toJSON)

    val documents = spark.sparkContext.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    Thread.sleep(1000)
    MongoSpark.save(documents) // Uses the SparkConf for configuration
    //    println(rdd.first.toJson)

    /*  val spark = SparkSession.builder
        .master("local")
        .appName("mongodb")
        .getOrCreate()
      val inputUri="mongodb://192.168.10.192:27017/test.copyqa"
      val df = spark.read.format("com.mongodb.spark.sql").options(
        Map("spark.mongodb.input.uri" -> inputUri,
          "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
          "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
          "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
        .load()

      df.take(10).foreach(println)*/

  }

  def testJoin(): Unit = {
    val (sparkSession, sc) = createSparkSession()
    import sparkSession.implicits._

    // RDD如何转换为DS DF
    //如何构造DS DF
    //    val df1 = Seq(1, 2, 3, 4).toDF()
    //    val df2 = Seq(5, 6, 7, 8).toDF()
    //    df1.join(df2).show()

    val rdd1 = sc.parallelize(Array(("A", 1), ("B", 2), ("B", 5), ("B", 6)))
    val rdd2 = sc.parallelize(Array(("B", 3), ("C", 4), ("D", 2), ("B", 7)))
    val rdd3 = sc.makeRDD(Array(("B", 4), ("C", 5), ("B", 2)))
    rdd1.collect().foreach(println _)
    rdd2.collect().foreach(println _)
    println("join 结果：")
    rdd1.join(rdd2).join(rdd3).collect.foreach(println _)

    sc.makeRDD(Seq((1, 2), (3, 4))).join(sc.makeRDD(Seq((5, 6), (7, 8))))
    sc.makeRDD(Seq(("A", Seq(1, 2, 3, "D")), ("C", Seq(4, 5)))).join(sc.makeRDD(Seq(("A", Seq(6, 7)), ("A", "ddd"), ("B", Seq(8, 9))))).foreach(println _)
    sc.makeRDD(Array(("A", Seq(1, 2, 3, "D")), ("C", Seq(4, 5)))).join(sc.makeRDD(Seq(("A", Seq(6, 7)), ("A", "ddd"), ("B", Seq(8, 9))))).foreach(println _)
    sc.makeRDD(List(("A", Seq(1, 2, 3, "D")), ("C", Seq(4, 5)))).join(sc.makeRDD(Seq(("A", Seq(6, 7)), ("A", "ddd"), ("B", Seq(8, 9))))).foreach(println _)

    //sc.makeRDD(Seq(Map("A" -> Seq(1, 2, 3, "D")), Map("C"->Seq(4, 5)))).join(sc.makeRDD(Seq(Map("A"->Seq(6, 7)), Map("A"->"ddd"), Map("B"->Seq(8, 9))))).foreach(println _)
    //join 处理的RDD需要是元素为Tuple2类型的collection

    println("-----测试左外连接------")
    //    this 并 (this 交 that)
    rdd1.leftOuterJoin(rdd2).collect().map(println _)
    //左外连接以左为主，如果that有值则按照Option类型加入，如果在that中未匹配到值，则按照None保存
    println("-----测试右外连接------")
    //that 并 (this 交 that)
    rdd1.rightOuterJoin(rdd2).collect().foreach(println _)
    println("-----测试全外连接------")
    rdd1.fullOuterJoin(rdd2).collect().foreach(println _)

    sparkSession.stop()
  }

  def createSparkSession(): (SparkSession, SparkContext) = {
    val sparkSession = SparkSession.builder().master("local").appName("localTest").getOrCreate()
    val sc = sparkSession.sparkContext
    (sparkSession, sc)
  }

  def testDataFrame(): Unit = {
    val (sparkSession, sc) = createSparkSession()
    val df1 = sparkSession.createDataFrame(List(Person("jack", 23), Person("tom", 10), Person("tom", 101), Person("tom", 12), Person("tom", 13)))
    val df3 = sparkSession.createDataFrame(List(Person("zhangsan", 23), Person("tom", 10)))
    df1.show()
    df1.printSchema()
    val df2 = sparkSession.createDataFrame(Seq(Student(1, "aaa", 10), Student(2, "bbb", 20)))
    df2.show()
    df1.show(1)
    df1.show(2)
    df1.show(3)
    df1.show(4)
    println("====test slect====")
    df1.select("name").show()
    //查询不存在的列会报错
    //df1.select("noLine").show()

    df1.filter(_.getString(0) == "tom").show()
    import sparkSession.implicits._
    val ds1 = df1.as[Person]
    ds1.filter($"name" === "tom").select("age").show()
    ds1.groupBy("name").count().show()


    //TODO 测试 join 连接查询
    //    df1.join(df3).show()
  }

  def testStreaming(): Unit = {
    import org.apache.spark.streaming._
    val (sparkSession, sc) = createSparkSession()
    val sparkStream = new StreamingContext(sc,Seconds(1))
  }


}
