package sparktest

/**
  * Created by PerkinsZhu on 2018/8/18 15:01
  **/

import java.io.File

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

object WordCount {
  private val master = "spark://192.168.10.156:7077"
  private val remote_file = "hdfs://192.168.10.156:9000/test/input/test.txt"
  val logger = Logger.getLogger(WordCount.getClass)
  private val dir = System.getProperty("user.dir")
  private val sc = new SparkContext(new SparkConf())

  def testSpark(): Unit = {
    println("---startTask--")

    //这些配置就相当于在程序中执行submit操作,运行时可以直接通过java -jar来运行
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster(master)
      .set("spark.executor.memory", "900m")
      .setJars(List(dir + File.separator + "workTestWithSpark.jar"))

    val sc = new SparkContext(conf)
    val textFile = sc.textFile(remote_file)
    textFile.take(1000).foreach(item => {
      logger.info(item)
      println(item)
    })
    println("task is over")
  }

  def testSubmit() = {
    println("---submit test--")
    //如果使用submit命令，则这里可以取消sparktest.WordCount.testSpark中的配置
    val textFile = sc.textFile(remote_file)
    textFile.take(1000).foreach(item => {
      logger.info(item)
      println(item)
    })
    println("task is over")
  }

  def testCount() = {
    sc.textFile(remote_file).flatMap(line => line.split("\t")).map((_, 1)).reduceByKey((_ + _)).collect().foreach(println)
  }

  def main(args: Array[String]) {
    logger.info("-----task start-----")
    //    testSpark()
    //    testSubmit()
    //    testCount()
    logger.info("-----task over-----")
  }
}