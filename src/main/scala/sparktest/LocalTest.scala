package sparktest

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by PerkinsZhu on 2018/8/22 10:07
  **/
object LocalTest {
  private val master = "spark://192.168.10.156:7077"
  private val remote_file = "hdfs://192.168.10.156:9000/test/input/test.txt"
  val logger = Logger.getLogger(SubmitTest.getClass)

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
}
