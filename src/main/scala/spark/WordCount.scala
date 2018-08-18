package spark

/**
  * Created by PerkinsZhu on 2018/8/18 15:01
  **/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  private val master = "spark://192.168.10.156:7077"
  private val remote_file = "hdfs://192.168.10.156:9000/test/input/log1.txt"
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster(master)
      .set("spark.executor.memory", "512m")
      .setJars(List("F:\\myCode\\workTestWithSpark\\classes\\artifacts\\workTestWithSpark_jar\\workTestWithSpark.jar"))

    val sc = new SparkContext(conf)
    val textFile = sc.textFile(remote_file)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}