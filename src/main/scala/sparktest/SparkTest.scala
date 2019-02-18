package sparktest

import sparktest.kafka.StreamingWithKafka

/**
  * Created by PerkinsZhu on 2018/8/22 10:21
  **/
object SparkTest {
  def main(args: Array[String]): Unit = {
    //    LocalTest.testSpark()
    //    SubmitTest.testMongod()
    //    LocalTest.testDataFrame()
    //    StreamingWithKafka.testKafkaStreaming()
    //    StreamingWithKafka.readAndWriteWithKafka()
//        SubmitTest.testSpark()
//        LocalTest.baseTest()
    LocalTest.testHive()
    //    LocalTest.testDataFrame()
  }
}
