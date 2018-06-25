package spark.mongodb

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.junit.{After, Test}
/**
  * Created by PerkinsZhu on 2018/6/22 17:28
  **/
class MongodbTest {
  System.setProperty("hadoop.home.dir", "E:\\Program Files\\hadoop-2.9.1")
  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb://perkins:perkins@127.0.0.1:27017/test.test")
    .config("spark.mongodb.output.uri", "mongodb://perkins:perkins@127.0.0.1:27017/test.test")
    .getOrCreate()
  val sc = spark.sparkContext

  @After
  def stopSparkContext(): Unit = {
    if (!sc.isStopped) {
      sc.stop()
    }
  }
  @Test
  def testMongoWrite(): Unit = {
    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    MongoSpark.save(documents)
  }

  @Test
  def testRead(): Unit = {
    val data = MongoSpark.load(sc)
    data.foreach(println)
  }
}
