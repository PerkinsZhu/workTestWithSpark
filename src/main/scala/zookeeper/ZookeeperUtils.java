package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;

public class ZookeeperUtils {

    String path = "/zoo1";

    public ZooKeeper createZookeeper(Watcher watcher) throws IOException {
        //多个ip使用，号分割,中间不允许存在空格
        return new ZooKeeper("192.168.10.156:2181,192.168.10.158:2181,192.168.10.162:2181", 20000, watcher);
    }

    @Test
    public void testDemo() throws Exception {
        Watcher watcher = event -> System.out.println("option\t接收到监控事件\ttype:" + event.getType() + "\t path:" + event.getPath());
        ZooKeeper zooKeeper = createZookeeper(watcher);
        this.createNode(zooKeeper, path);
        System.out.println("get data");
        zooKeeper.getData("/zoo1", watcher, null);
        System.out.println("set  hello");
        zooKeeper.setData("/zoo1", "hello".getBytes(), -1);
        System.out.println("获取第一次节点信息：" + new String(zooKeeper.getData("/zoo1", watcher, null), "UTF-8"));
        System.out.println("set  world");
        zooKeeper.setData("/zoo1", "world".getBytes(), -1);
        System.out.println("获取第二次数据信息：" + zooKeeper.getData("/zoo1", watcher, null));
        System.out.println("节点状态：" + zooKeeper.getState());
        //        zooKeeper.delete("/zoo1", -1);
        System.out.println(zooKeeper.exists("/zoo1", false));
        zooKeeper.close();
    }


    public void createNode(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, true);   //会设置watch
        if (stat == null) {
            zooKeeper.create(path, path.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }


    @Test
    public void testWriter() throws Exception {
        Watcher watcher = event -> System.out.println("threadWriter\t接收到监控事件\ttype:" + event.getType() + "\t path:" + event.getPath() + "\t state:" + event.getState());
        ZooKeeper zooKeeper = createZookeeper(watcher);
        createNode(zooKeeper, path);
        new Thread(() -> {
            int i = 0;
            while (true) {
                try {
                    String data = ("info-" + (i++));
                    System.out.println("更新节点数据:" + data);
                    zooKeeper.setData("/zoo1", data.getBytes(), -1);
                    String newNode = path + "/" + data;
                     zooKeeper.exists(newNode,watcher);//添加子节点事件
                    zooKeeper.getData(path,watcher,null);//添加节点修改事件
                    Thread.sleep(100);
                    System.out.println("添加子节点:" + newNode);
                    zooKeeper.create(newNode, "init".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }).start();
        Thread.sleep(10000000);
    }

    @Test
    public void startTaskOtherListener() throws Exception {
        Watcher watcher = event -> System.out.println("threadListener\t接收到监控事件\ttype:" + event.getType() + "\t path:" + event.getPath());
        ZooKeeper zooKeeper = createZookeeper(watcher);
        this.createNode(zooKeeper, path);
        new Thread(() -> {
            while (true) {
                try {
                    //这里设置的watch在下次写入的时候会被触发进行回调。只会回调一次，所以使用的时候，对每一zooKeeper调用都要设置watch
                    String result = new String(zooKeeper.getData(path, watcher, null), "UTF-8");
                    System.out.println("获取到新数据:" + result);
                    //zooKeeper.getChildren(path, watcher).forEach((String str) -> System.out.println(str));
                    zooKeeper.exists(path + "/" + result, watcher);
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }).start();

        Thread.sleep(10000000);
    }


    @Test
    public void testCreateDoubleNode() {
        try {
            Watcher watcher = event -> System.out.println("threadListener\t接收到监控事件\ttype:" + event.getType() + "\t path:" + event.getPath());
            ZooKeeper zooKeeper = createZookeeper(watcher);
            for (int i = 0; i < 5; i++) {
                new Thread(() -> {
                    try {
                        String newPath = path + "/lock";
                        String  result =  zooKeeper.create(newPath , path.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        if(newPath.equals(result)){
                            System.out.println("节点创建成功");
                        }else{
                            System.out.println("节点创建畸形");
                        }
                    } catch (Exception e) {
                        if(e instanceof  KeeperException.NodeExistsException){
                            System.out.println("该节点已存在");
                        }else{
                            System.out.println("创建节点发生未知异常");
                        }

                    }
                }).start();
            }
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/**
 * zooKeeper就像(是)一个分布式的共享变量池，分布式服务可以在里面放入共享变量，各个子节点服务从zookeeper中读写数据，协调工作。
 *  分布式服务可以利用zookeeper数据的强一致性来实现分布式锁技术
 *  ·应用场景：https://www.cnblogs.com/oxspirt/p/7427969.html
 *
 * exists、getChildren  getData 可以设置watch
 * create、set 可以触发watch事件
 *
 *      分布式锁：
 *          1、当有两个线程同时去持有锁的时候发生错误，该如何解决？
 *              例如：A线程去查询Node(".../lock"),发现值为0(未持有)，然后修改Node("../lock")为1,此时，在修改之前，B线程也查询Node发现为0，这是B也去
 *              修改Node，这样就会导致两个线程都去持有锁。
 *            解决方法：不要根据Node("../lock")的Value值来判断是否被持有，而是根据是否存在该Node来判断该锁是否被持有。
 *                  这样，当A和B去查询发现没有的时候，然后去创建Node,这是，肯定会有一个失败，如果失败，则等待去持有锁
 *                  如果成功，则成功持有锁。这里利用的时候创建时存在Node会发生异常的特性。见测试案例:testCreateDoubleNode
 *
 */

