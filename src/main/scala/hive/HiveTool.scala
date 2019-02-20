package hive

import druidtest.DruidPoolUtils

/**
  * Created by PerkinsZhu on 2019/2/20 17:35
  **/
object HiveTool {
  def main(args: Array[String]): Unit = {
    val connection = DruidPoolUtils.getConnection
    connection.map(con => {
      val stmt = con.prepareStatement("select * from company")
      val rs = stmt.executeQuery()
      while (rs.next()) {
        println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3))
      }
    })

  }

}
