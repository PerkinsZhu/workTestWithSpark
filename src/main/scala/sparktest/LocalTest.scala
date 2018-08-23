package sparktest

import com.mongodb.spark.MongoSpark
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * Created by PerkinsZhu on 2018/8/22 10:07
  **/
object LocalTest {
  private val master = "spark://192.168.10.156:7077"
  private val remote_file = "hdfs://192.168.10.156:9000/test/input/test.txt"
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
}
