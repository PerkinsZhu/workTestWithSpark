package hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, TableDescriptor}
import scala.collection.JavaConverters._

/**
  * Created by PerkinsZhu on 2018/8/22 17:42
  **/
object TestUtil {
  val conf = HBaseConfiguration.create()
  //TODO 这里面总是会取到localhost地址，导致无法链接hbase master.
  //应该是hbase 配置问题，暂时未解决
  conf.set("hbase,zookeeper.quorum", "192.168.10.156")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  val connect = ConnectionFactory.createConnection(conf)
  val admin = connect.getAdmin

  def createTable(tableName: String): Unit = {
    val isExists = admin.tableExists(TableName.valueOf(tableName))
    println(isExists)
  }

  def main(args: Array[String]): Unit = {
    //    createTable("test")
    admin.listTableDescriptors().asScala.foreach(println(_))
  }

}
