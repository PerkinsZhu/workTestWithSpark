package hbase

import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Created by PerkinsZhu on 2018/8/22 17:42
  **/
class TestUtil {
  val conf = HBaseConfiguration.create()
  //这里面总是会取到localhost地址，导致无法链接 hbase master.
  // 解决办法:在scala 目录下加入hbase-site.xml ,然后配置上  hbase,zookeeper.quorum 即可
  val connect = ConnectionFactory.createConnection(conf)
  val admin = connect.getAdmin
  val table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf("my_ns:javaTable"))

  @Test
  def createNameSpace(): Unit = {
    import org.apache.hadoop.hbase.NamespaceDescriptor
    val nsDesc = NamespaceDescriptor.create("").build
    admin.createNamespace(nsDesc)
  }

  @Test
  def testListTable(): Unit = {
    admin.listTableNames().foreach(println _)

    admin.listNamespaceDescriptors().foreach(println _)
  }


  @Test
  def createTable(): Unit = {
    val tableName = TableName.valueOf("my_ns:javaTable")
    if (admin.tableExists(tableName)) { // 如果存在要创建的表，那么先删除，再创建
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
      System.out.println(tableName + " is exist,detele....")
    }
    /* 2.1.0版本语法
      val tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        val list = List(ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes()).build(),
          ColumnFamilyDescriptorBuilder.newBuilder("cf2".getBytes()).build(),
          ColumnFamilyDescriptorBuilder.newBuilder("cf3".getBytes()).build()).asJava
        tableDescriptor.setColumnFamilies(list)
        admin.createTable(tableDescriptor.build())
      */

    val tableDescriptor = new HTableDescriptor(tableName);
    tableDescriptor.addFamily(new HColumnDescriptor("column1"));
    tableDescriptor.addFamily(new HColumnDescriptor("column2"));
    tableDescriptor.addFamily(new HColumnDescriptor("column3"));
    admin.createTable(tableDescriptor);

    System.out.println("end create table ......")
  }

  @Test
  def testInsertData() {
    println("start insert data ......");
    val put = new Put("rowKey-03".getBytes());
    put.addColumn("column1".getBytes(), "name".getBytes(), "aaa".getBytes())
    put.addColumn("column1".getBytes(), "age".getBytes(), "aaa".getBytes())
    put.addColumn("column1".getBytes(), "sex".getBytes(), "aaa".getBytes())
    put.addColumn("column2".getBytes(), "age".getBytes(), "bbb".getBytes())
    put.addColumn("column3".getBytes(), "sex".getBytes(), "ccc".getBytes())
    table.put(put);

    println("end insert data ......");
  }

  @Test
  def testDeleteRow(): Unit = {
    import java.util
    val list = new util.ArrayList[Delete]
    list.add(new Delete("112233bbbcccc".getBytes()))
    list.add(new Delete("rowKey-01".getBytes()))
    table.delete(list)
  }

  @Test
  def testQuery(): Unit = {
    val tableData = table.getScanner(new Scan())
    tableData.asScala.foreach(result => {
      result.getFamilyMap("column1".getBytes()).asScala.foreach(temp => {
        println(new String(temp._1, "UTF-8") + "--->" + new String(temp._2, "UTF-8"))
      })
      println(new String(result.getRow, "UTF-8"))
      result.listCells().asScala.foreach(println _)
    })
  }

  @Test
  def testGet(): Unit = {
    val get = new Get("rowKey-03".getBytes())
    table.get(get).listCells().asScala.foreach((cell: Cell) => {
      println(cell)
    })
  }

  @Test
  def testFilter(): Unit = {
    val filter = new SingleColumnValueFilter(Bytes.toBytes("column1"), "age".getBytes(), CompareOp.EQUAL, Bytes.toBytes("aaa")); // 当列column1的值为aaa时进行查询
    val scan = new Scan().setFilter(filter)
    table.getScanner(scan).asScala.foreach(result => result.listCells().asScala.foreach(println(_)))
  }

  /** 参考：https://www.cnblogs.com/JingJ/p/4521245.html
    * 不同的列族数据会写入不同的文件，同一列族数据会写入同一文件
    *
    */
}