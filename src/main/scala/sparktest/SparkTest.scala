package sparktest

import sparktest.kafka.StreamingWithKafka
import sparktest.sparksql.SparkSQLTest

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
    //    SubmitTest.testSpark()
    //            SubmitTest.testNetWorkWordCount()
    //        LocalTest.baseTest()
    //        LocalTest.testHive()
    //    LocalTest.testDataFrame()

    //        SubmitTest.testHive()
    //    SparkSQLTest.testBase()
    //    SparkSQLTest.testUDAF()
    //    SparkSQLTest.testRDD()
    //    SparkSQLTest.testAggregation()
    SubmitTest.testClusterHive()
  }
}
