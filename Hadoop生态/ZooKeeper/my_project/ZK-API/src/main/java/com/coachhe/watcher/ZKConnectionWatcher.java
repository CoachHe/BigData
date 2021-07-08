package com.coachhe.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZKConnectionWatcher implements Watcher {

    //计数器对象
    static CountDownLatch countDownLatch = new CountDownLatch(1);
    //连接对象
    static ZooKeeper zooKeeper;
    //连接的IP
    static String IP = "10.211.55.100:2181";

    public static void main(String[] args) {
        try {
            zooKeeper = new ZooKeeper(IP, 5000, new ZKConnectionWatcher());
            // 阻塞线程，等待连接的创建
            countDownLatch.await();
            // 会话id
            System.out.println(zooKeeper.getSessionId());
            Thread.sleep(50000);
            zooKeeper.close();
            System.out.println("结束");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

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
}
