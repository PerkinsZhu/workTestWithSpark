package sparktest.remote

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by PerkinsZhu on 2019/2/13 19:21
  **/
object RemoteTask {
  def baseTest(): Unit = {

    val conf = new SparkConf().setAppName("Spark Remote Test").setMaster("spark://192.168.10.163:7077").setJars(Seq("F:\\myCode\\workTestWithSpark\\classes\\artifacts\\remoteTest\\workTestWithSpark.jar"))
    //  val spark = new SparkContext(conf)
    val sparkSession = SparkSession.builder.config(conf = conf).getOrCreate()


    val logFile = "G:\\test\\spark\\people.txt" // Should be some file on your system
    val logData = sparkSession.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sparkSession.stop()
  }

}
