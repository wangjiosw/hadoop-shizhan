package zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;

import java.io.IOException;

public class ZookeeperDemo {
    @Before
    public void connect() throws IOException {
        String connectStr = "node002:2181,node003:2181,node004:2181";
        ZooKeeper zk = new ZooKeeper(connectStr,3000,null);
    }
}
