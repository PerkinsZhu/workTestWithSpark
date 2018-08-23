package sparktest.examples

import java.util.Properties

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import sparktest.examples.SparkContextTest.Student

/**
  * Created by PerkinsZhu on 2018/8/23 13:53
  **/
object SparkContextTest {

  val sparkSession = SparkSession.builder().master("local")
    .appName("test01").getOrCreate()

  @Test
  def testDoubleSparkContext(): Unit = {
    val sparkSession01 = SparkSession.builder().master("local")
      .appName("test01").getOrCreate()
    val sparkSession02 = SparkSession.builder().master("local")
      .appName("test01").getOrCreate()
    println(sparkSession01.hashCode())
    println(sparkSession02.hashCode())
    println(sparkSession01 eq sparkSession02)
    println(sparkSession01 == sparkSession02)

    val sparkConf = new SparkConf().setAppName("test03").setMaster("local")
    val sparkContext03 = SparkContext.getOrCreate(sparkConf)
    println(sparkSession01.sparkContext.hashCode())
    println(sparkSession02.sparkContext.hashCode())
    println(sparkContext03.hashCode())
    println(sparkContext03 == sparkSession01.sparkContext)
    println(sparkContext03 eq sparkSession01.sparkContext)
    println(sparkContext03.startTime == sparkSession01.sparkContext.startTime)

    sparkContext03.listJars().foreach(println)


  }


  @Test
  def testReadJson(): Unit = {
    val path = "G:\\test\\jsonFile\\temp.json"
    val data = sparkSession.read
      //注意，当multiLine == true时，spark在读取json的时候，只会读出第一个JsonValue对象。其默认一个文件只有一个JsonValue。如果多个则必须保证一行一个JsValue，然后去掉multiLine选项
      .option("multiLine", true)
      .option("mode", "PERMISSIVE")
      .json(path)
    data.printSchema()
    data.show()
    sparkSession.stop()
  }

  @Test
  def testMultiJsonValue(): Unit = {
    val path = "G:\\test\\jsonFile\\temp.json"
    //    val path = "G:\\test\\jsonFile\\person.json"
    val data = sparkSession.read.json(path)
    data.show()
    data.foreach(line => println(line))

    data.filter(_.getString(0) == "user-5a2f9c1ed76b7dd2009404a7").select("robotId", "questions.question").foreach(println(_))
    //        data.filter($"_id" = "user-5a2f9c1ed76b7dd2009404a6").show()
    data.createOrReplaceTempView("qa")
    data.createGlobalTempView("globalqa") // 设置为global可以在多个 session中共享
    sparkSession.sql("SELECT * FROM qa WHERE updateTime = \"1513069598663\"").show()
    sparkSession.sql("SELECT * FROM global_temp.globalqa WHERE updateTime = \"1513069598663\"").show()
    sparkSession.newSession().sql("SELECT * FROM global_temp.globalqa WHERE updateTime = \"1513069598663\"").show()

  }

  case class Person(name: String, age: Long)
  case class Student(id:Int,name: String, age: Long)

  // 使用junit测试会失败，在main方法中调用该方法
  def testPerson(): Unit = {
    import sparkSession.implicits._
    val path = "G:\\test\\jsonFile\\person.json"
    val data = sparkSession.read.json(path)
    val personDfF = Seq(Person("jack", 23), Person("tom", 120)).toDS()
    personDfF.show()
    personDfF.map(_.age * 10).show()

    //注意里面的person.age需要用Long类型来接收，如果是int，则会报出转换异常
    data.as[Person].show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    data.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect().foreach(println(_))

  }

  def testSchema(): Unit = {
    import org.apache.spark.sql.types._
    val path = "G:\\test\\spark\\people.txt"

    //注意这里读出的是RDD[String]， 不是DataSet
    val data = sparkSession.sparkContext.textFile(path)
    import sparkSession.implicits._

    val rowData = data.map(_.split(",")).map(line => Row(line(0), line(1).trim))
    val schema = StructType(Seq(StructField("name", StringType, false), StructField("age", StringType, false)))

    val schemaString = "name age"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema2 = StructType(fields)

    val rowRDD = data.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))

    val df = sparkSession.createDataFrame(rowRDD, schema2)
    df.show()
    df.createTempView("person")
    val result = sparkSession.sql("SELECT * FROM person").map(_.getString(0))
    result.show()
    result.write.format("parquet").mode(SaveMode.Append).save("G:\\test\\spark\\out")

    //读取parquet数据
    sparkSession.read.parquet("G:\\test\\spark\\out").foreach(println(_))

  }


  def testJDBC(): Unit = {
    val dbUrl = "jdbc:mysql://localhost:3306/test?user=root&password=jinzhao&serverTimezone=GMT%2B8"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "jinzhao")

    //第一种加载方式
    /*   val jdbcDF = sparkSession.read
         .format("jdbc")
         .option("url", "jdbc:mysql://localhost:3306/test?user=root&password=jinzhao&serverTimezone=GMT%2B8")
         .option("dbtable", "person")
         .option("user", "root")
         .option("password", "jinzhao")
         .load()*/

    //第二种加载方式
    val jdbcDF = sparkSession.read.jdbc(dbUrl, "person", connectionProperties)
    jdbcDF.take(10).foreach(println(_))

    //保存RDD到数据库表中
    // 第一种写入方式
    import sparkSession.implicits._
    // Seq(Person("aaaa", 23)).toDS().write
    jdbcDF.write
      .format("jdbc")
      .option("url", dbUrl)
      .option("dbtable", "personnew2")
      .option("user", "root")
      .option("password", "jinzhao").mode(SaveMode.Append)
      .save()
    //  第二种写入方式
    jdbcDF.write.mode(SaveMode.Append).jdbc(dbUrl, "personnew2", connectionProperties)

    // 自定义schema 格式读
    //    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING,age DECIMAL(2,0)")
    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
    val jdbcDF2 = sparkSession.read.jdbc(dbUrl, "personnew2", connectionProperties)
    println("自定义读格式")
    jdbcDF2.foreach(println(_))

    println("自定义格式写")
    Seq(Student(1,"aaa", 11),Student(2,"bbb", 22), Student(3,"ccc", 33)).toDS().write.option("createTableColumnTypes", "id INT, name VARCHAR(11), age DECIMAL(2)").mode(SaveMode.Append)
      .jdbc(dbUrl, "student02", connectionProperties)

  }

  def main(args: Array[String]): Unit = {
    //    testPerson()
    //    testSchema()
    testJDBC()
  }

}
