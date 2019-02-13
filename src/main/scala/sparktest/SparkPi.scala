package sparktest

/**
  * Created by PerkinsZhu on 2019/2/13 17:27
  **/

import scala.math.random
import org.apache.spark._

object SparkPi {
  def main(args: Array[String]) {
    // 先build在本地生成jar包，然后运行该程序即可执行
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://192.168.10.163:7077").setJars(Seq("F:\\myCode\\workTestWithSpark\\classes\\artifacts\\remoteTest\\workTestWithSpark.jar"))
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    println("Time:" + spark.startTime)
/*    val n = math.min(1000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)*/
//    println("Pi is roughly " + 4.0 * count / n)

    1 to 100 foreach (i => println("----" + i))
    spark.stop()
  }
}
