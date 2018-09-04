package kafka.scala

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer._
import org.junit.Test
import scala.collection.JavaConverters._
/**
  * Created by PerkinsZhu on 2018/9/3 14:02
  **/
class KafkaUtil {

  def getProperties(): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "servera.local.com:9092,serverb.local.com:9092,serverc.local.com:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("retries", "3")
    properties.put("acks", "1")
    properties
  }

  @Test
  def testPublish(): Unit = {
    val properties = getProperties()
    val producer: Producer[String, String] = new KafkaProducer(properties)
    new Thread(new Runnable {
      override def run(): Unit = {
        var count = 0
        while (true) {
          val msg = s"jack--$count"
          producer.send(new ProducerRecord[String, String]("User", msg), new Callback {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = println(msg + "-->发送" + {if (e == null) {"成功"} else {"失败"}})
          })
          Thread.sleep(1000)
          count = count + 1
        }
      }
    }).start()
    Thread.sleep(Int.MaxValue)
  }

  @Test
  def testSubscribe(): Unit = {
    val properties = getProperties()
    properties.setProperty("group.id","3")
    /**
      * 该配置必须在该groud.id第一次执行时生效
      *     offset 同一个group中不同的consumer共享，因此，加入group01.consumer01 结束时，offset=100。那么在运行group01.consumer02的时候 ，offset就从100开始
      *     不会重新从0 开始
      *     * 当找不到offset时，使用什么策略来生成offset
      *   earliest、latest、none、anything
      *   见:https://blog.csdn.net/joy6331/article/details/51180467
      */
    properties.setProperty("auto.offset.reset","earliest")
    val topics = util.Arrays.asList("spark-User")
    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(topics)
//    consumer.assign(util.Arrays.asList(new TopicPartition("User",0)))
//    consumer.seekToBeginning(util.Arrays.asList(new TopicPartition("User",0)))
//    consumer.seek(new TopicPartition("User",0),0)
//    consumer.beginningOffsets(topics)
    while (true) {
      val consumerRecords = consumer.poll(Duration.ofMillis(1000))
      consumerRecords.asScala.foreach(record => {
        println(record)
      })
      Thread.sleep(1000)
    }
    Thread.sleep(Int.MaxValue)
  }

    //TODO  查看offset使用方式 如何获取 设置
}
