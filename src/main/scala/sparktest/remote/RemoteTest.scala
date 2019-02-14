package sparktest.remote

/**
  * Created by PerkinsZhu on 2019/2/13 19:20
  **/
object RemoteTest {
  def main(args: Array[String]): Unit = {
    println("==========开始执行spark 任务=============")
    RemoteTask.baseTest()
    println("==========spark 任务执行结束=============")
  }
}
