package basetest

import org.junit.Test

/**
  * Created by PerkinsZhu on 2018/8/20 16:31
  **/
class TestUtil {
  @Test
  def testReduce(): Unit = {

    println("tom\tjack\ttomc\tjack".split("\t").reduce((a, b) => {a + b}))
    List("tom\tjack\ttomc\tjack", "aa\tbb\tcc").map(_.split("\t")).reduce((a, b) => a.++(b.map(_ + "--"))).foreach(println)
    val data = List("tom\tjack\ttomc\tjack", "aa\tbb\tcc").flatMap(_.split("\t")).map((_, 1))
    data.reduce((a, b) => {
      println(a)
      println(b)
      a
    })


  }

  @Test
  def testEq(): Unit = {
    //不要直接比较字符串，因为字符串 在jvm中是一个存再常量池中的常量，如果已经存在，则不会开辟新的地址存储
    println(Some("a") == Some("a"))
    println(Some("a") eq Some("a"))
    println(Some("a") ne Some("a"))
  }

}
