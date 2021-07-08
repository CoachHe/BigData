package com.coachhe.案例;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class GloballyUniqueId implements Watcher {
    String IP = "10.211.55.100:2181";
    static ZooKeeper zooKeeper;
    String defaultPath = "/uniqueId";
    CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent watchedEvent) {
        // 事件类型
        if (watchedEvent.getType() == Event.EventType.None) {
            if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                System.out.println("连接创建成功!");
                //继续往下执行
                countDownLatch.countDown();
            } else if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
                System.out.println("断开连接！");
            } else if (watchedEvent.getState() == Event.KeeperState.Expired) {
                System.out.println("会话超时!");
            } else if (watchedEvent.getState() == Event.KeeperState.AuthFailed) {
                System.out.println("认证失败!");
            }
        }
    }

    // 构造方法
    public GloballyUniqueId() {
        try {
            // 创建连接对象
            zooKeeper = new ZooKeeper(IP, 5000, this);
            // 阻塞线程，等待连接创建成功
            countDownLatch.await();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    //生成ID的方法
    public String getUniqueId(){
        String path = "";
        try {
            path = zooKeeper.create(defaultPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return path.substring(9);
    }


    public static void main(String[] args) {
        GloballyUniqueId globallyUniqueId = new GloballyUniqueId();
        for (int i = 1; i <= 5; i++) {
            String id = globallyUniqueId.getUniqueId();
            System.out.println(id);
        }


    }

}
