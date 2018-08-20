package com.zhenquan.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class DistributedServer {
    public static final String connectString = "mini:2181,mini1:2181,mini2:2181";
    private static final int SESSION_TIMEOUT = 30000;
    private ZooKeeper zk = null;
    private static final String parentNode = "/servers";
    /**
     * 创建到zk的客户端连接
     */
    public void getConnet() throws IOException {
        zk = new ZooKeeper(connectString, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "------" + watchedEvent.getPath());
                try {
                    zk.getChildren("/", true);
                } catch (Exception e) {
                }
            }
        });
    }

    public void registerServer(String hostname) throws KeeperException,InterruptedException {
        Stat exists = zk.exists(parentNode, false);
        if (exists == null) {
            String create = zk.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        }
    }
}
