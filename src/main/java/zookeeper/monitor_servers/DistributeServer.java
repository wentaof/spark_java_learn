package zookeeper.monitor_servers;
import org.apache.zookeeper.*;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/22 11:08
 * @Version 1.0.0
 * @Description TODO
 */
public class DistributeServer {
    static String connectstr = "test:2181";
    static int sessionTimeout = 2000;
    ZooKeeper zkCli = null;
    String parentNode = "/servers";
    public void getConnection() throws Exception{
        zkCli = new ZooKeeper(connectstr, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {

            }
        });
    }

    public void registerServer(String hostname) throws Exception{
        String create = zkCli.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(hostname + " is online "+ create);
    }

//    业务功能
    public void bussine(String hostname) throws Exception{
        System.out.println(hostname + "is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        DistributeServer server = new DistributeServer();
        server.getConnection();
        server.registerServer("node1");
        server.bussine("node1");
    }

}
