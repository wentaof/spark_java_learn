package zookeeper;

import org.apache.parquet.io.ValidatingRecordConsumer;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/22 10:40
 * @Version 1.0.0
 * @Description TODO
 */
public class learn_zk {
    static String connectstr = "test:2181";
    static int sessionTimeout = 2000;
    ZooKeeper zkCli = null;

    @Before
    public void init() throws Exception{
        zkCli = new ZooKeeper(connectstr, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {
                //    收到事件通知后的回调函数(用户的业务逻辑)
                System.out.println(event.getType() + "-->" + event.getState() + "-->" + event.getPath() + "-->" + event.getWrapper().toString());
                try {
                    zkCli.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

//    创建子节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        String s = zkCli.create("/atguigu", "xiaoshuai".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(s);
    }

//    获取子节点并监听节点变化
    @Test
    public void getChildren() throws Exception{
        List<String> children = zkCli.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

//    判断znode是否存在
    @Test
    public void exist() throws Exception{
        Stat exists = zkCli.exists("/sanguo", false);
        System.out.println(exists);
        System.out.println(exists == null ? "not exist" : "exist");
    }


}
