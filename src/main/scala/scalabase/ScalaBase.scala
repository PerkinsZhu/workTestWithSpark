package scalabase

import org.junit.Test

/**
  * Created by PerkinsZhu on 2018/6/25 11:05
  **/
class ScalaBase {

  @Test
  def testReduceByKey(): Unit = {
    val list = List.range(10, 100, 20)
    println(list.reduce((a, b) => a + b))
  }

}
