package sparktest.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by PerkinsZhu on 2018/9/4 10:11
  **/
object StreamingWithKafka {
  private val brokers = "servera.local.com:9092,serverb.local.com:9092,serverc.local.com:9092"

  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("sparkSql")
      //.config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()
    spark
  }


  def testKafkaStreaming(): Unit = {
    val spark = getSparkSession()
    import spark.implicits._
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      //通过option的方式设置zookeeper的参数
      //.option("auto.offset.reset","earliest")
      .option("subscribe", "User")
      .load()
    //kafka 传入的数据结果：
    //<key:binary,value:binary,topic:string,partition:int,offset:bigint,timestamp:timestamp,timestampType:int>
    df.select("key", "value").map(row => {
      val value = new String(row.getAs[Array[Byte]](1), "UTF-8")
      (row.getString(0), value)
    })
      .toDF("key", "value")
      .groupBy("value").count()
      //.groupBy("_2").count()  //如果未转化为DataFrame时，使用“_2”方式来取值，实际上是按照tuple的格式来进行转化的，key就是_1、_2
      .writeStream.outputMode(OutputMode.Complete())
      .format("console")
      .start().awaitTermination()
    Thread.sleep(Int.MaxValue)
  }

  //从kafka中读取数据，然后转存到kafka中
  def readAndWriteWithKafka(): Unit = {
    val spark = getSparkSession()
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", "User")
      .load().map(row => {"SPARK--->" + new String(row.getAs[Array[Byte]]("value"), "UTF-8")})
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", "spark-User")
      .option("checkpointLocation", "/temp/kafka")
      .start().awaitTermination()
    Thread.sleep(Int.MaxValue)
  }
}
