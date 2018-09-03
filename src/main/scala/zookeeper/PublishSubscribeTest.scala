package zookeeper

import java.nio.file.WatchEvent

import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}
import org.junit.{Before, Test}

/**
  * Created by PerkinsZhu on 2018/9/3 9:38
  *
  * 测试zookeeper的发布订阅模式
  * 发布订阅模式分为两种：
  * 在推模式中，服务器主动将数据更新发送给所有订阅的客户端。
  * 在拉模式中，由客户端主动发起请求来获取最新数据，客户端可以采用定时轮询的形式。
  **/
class PublishSubscribeTest {

  def initZooKeeper(watcher: Watcher): ZooKeeper = {
    new ZooKeeper("192.168.10.156:2181,192.168.10.158:2181,192.168.10.162:2181", 20000, watcher)
  }


  def createNode(zk: ZooKeeper, path: String): Unit = {
    val state = zk.exists(path, true)
    if (state == null) zk.create(path, "init".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  val path = "/zoo1/publishSubscribe"

  @Test
  def doPublish(): Unit = {
    val zk = initZooKeeper(new Watcher {
      override def process(watchedEvent: WatchedEvent): Unit = print(watchedEvent.getState)
    })
    createNode(zk, path)
    var count = 0;
    while (true) {
      val data = s"init$count"
      zk.setData(path, data.getBytes(), -1)
      println("修改为-->" + data)
      count = count + 1
      Thread.sleep(500)
    }
  }

  @Test
  def doSubscribe(): Unit = {
    var zk: ZooKeeper = null
    val stat = new Stat()

    def getData(): Unit = {
      val bytes = zk.getData(path, true, stat)
      println("获取到最新数据:" + new String(bytes, "UTF-8"))
    }

    zk = initZooKeeper(new Watcher {
      override def process(watchedEvent: WatchedEvent): Unit = {
        println(s"type:${watchedEvent.getType}\tstate:${watchedEvent.getState}\tpath:${watchedEvent.getPath}")
        if (Event.EventType.NodeDataChanged == watchedEvent.getType) {
          getData()
        } else {
          //为了避免订阅中断，对于其他类型事件，也要继续进行watch
          //NodeDeleted NodeCreated NodeDataChanged
          zk.exists(path, this)
        }
      }
    })
    createNode(zk, path)
    //该句存在的目的是为了注册watch，注册之后有数据变动时就会触发process
    val bytes = zk.getData(path, true, stat)
    println("初始化订阅数据:" + new String(bytes, "UTF-8"))
    Thread.sleep(Int.MaxValue)
  }


}
