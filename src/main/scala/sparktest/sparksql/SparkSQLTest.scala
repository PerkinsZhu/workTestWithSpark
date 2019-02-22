package sparktest.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by PerkinsZhu on 2019/2/14 10:29
  **/
object SparkSQLTest {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("warn")

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  def testBase(): Unit = {
    val df = spark.read.json("/test/input/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()
    println("===========to sql =================")
    df.createTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()
    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people").show()
    spark.newSession().sql("select * from global_temp.people").show()
  }

  def testUDAF(): Unit = {
    spark.udf.register("myAverage", MyAverage) //注册自定义函数
    val df = spark.read.json("/test/input/employee.json")
    df.createOrReplaceTempView("employees")
    df.show()
    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
  }

  def testRDD(): Unit = {
    val accum = sc.longAccumulator("My Accumulator")
    var localCount = 0
    val df = sc.parallelize(1 to 100).map(_ * 10)
    df.foreach(row => {
      accum.add(1)
      localCount = localCount + 1
      println("foreach ->" + row)
    })
    df.foreachPartition(row => println("foreachPartition ->" + row.mkString))
    df.collect.foreach(row => println("collect foreach ->" + row))
    println("localCount" + localCount)
    println("accum" + accum.value)
    println("======================================")
    //已Partitions进行map循环操作。
    df.mapPartitions(ite => ite.map(_ + 1), false).foreach(i => println(s"mapPartitions--${i}"))
    df.mapPartitionsWithIndex((index, data) => {
      println("mapPartitionsWithIndex ---> " + index)
      data.map(_ + 1)
    }).collect().foreach(i => println(s"mapPartitionsWithIndex--${i}"))
    df.sample(false, 0.5).collect().foreach(i => println("sample -->" + i))
    val df2 = sc.parallelize(1 to 100).filter(_ % 2 == 0) map (_ * 10)
    df.union(df2).collect().foreach(i => println(s"union -->  ${i}"))
    val combinData = df.intersection(df2)
    combinData.collect().foreach(i => println(s"combin ---> ${i}"))
    combinData.distinct().collect().foreach(i => println(s"distinct ---> ${i}"))
    /*

        val reduceData = df.reduce((a, b) => a + b)
        println("reduce -->" + reduceData)
    */


    //    val mapData = df.map(i => (i % 10) -> i)
    //    mapData.aggregateByKey()

  }

  def testAggregation(): Unit = {
    val df = sc.parallelize(1 to 100).map(i =>(i % 10).toString -> i)
    df.collect().foreach(println(_))
    println("=====groupByKey=======")
    df.groupByKey().collect().foreach(data => println(data._1+"--->"+data._2.mkString("、")))
    println("=====reduceByKey=======")
    df.reduceByKey((k, v) => v * 10).collect().foreach(data => println(data._1 + "--->" + data._2))
    // df.aggregateByKey(100)()

    val broadcastVar = sc.broadcast(Array(1, 2, 3)) //在v广播之后不应修改对象 ，以确保所有节点获得相同的广播变量值（例如，如果稍后将变量发送到新节点）

  }

}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

object MyAverage extends UserDefinedAggregateFunction {
  // Data types of input arguments of this aggregate function
  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  // Data types of values in the aggregation buffer
  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  // The data type of the returned value
  def dataType: DataType = DoubleType

  // Whether this function always returns the same output on the identical input
  def deterministic: Boolean = true

  // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
  // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
  // the opportunity to update its values. Note that arrays and maps inside the buffer are still
  // immutable.
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // Updates the given aggregation buffer `buffer` with new input data from `input`
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // Calculates the final result
  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
}
