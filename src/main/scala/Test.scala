import com.mongodb.spark.MongoSpark
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by PerkinsZhu on 2018/5/27 10:19
  **/
class TestT {
  System.setProperty("hadoop.home.dir", "E:\\Program Files\\hadoop-2.9.1")
  val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    .set("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.test")
    .set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.test")
  val sc = new SparkContext(conf)

  @Test
  def testWordCount(): Unit = {
    val inputPath = "G:\\test\\test.txt"
    sc.textFile(inputPath).flatMap(line => line.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _).foreach(println)
    Thread.sleep(100000)
    sc.stop()
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
  import org.bson.Document
  @Test
  def testMongoWite(): Unit ={
    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    MongoSpark.save(documents)
  }
  @Test
  def testRead(): Unit ={
    val data  = MongoSpark.load(sc)
    data.foreach(println)
  }


}

