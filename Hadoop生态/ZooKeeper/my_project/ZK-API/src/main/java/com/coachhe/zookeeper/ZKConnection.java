package com.coachhe.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZKConnection {
    public static void main(String[] args) {
        try {
            // 计数器对象
            CountDownLatch countDownLatch = new CountDownLatch(1);
            // arg1:服务器ip和端口
            // arg2:客户端与服务器之间的会话超时时间（ms）
            // arg3:监视器对象
            ZooKeeper zooKeeper = new ZooKeeper("10.211.55.100:2181", 5000,
                    new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            if (event.getState() == Event.KeeperState.SyncConnected) {
                                System.out.println("连接创建成功");
                                // 通知countDownLatch不用继续阻塞了
                                countDownLatch.countDown();
                            }
                        }
                    });
            //主线程阻塞等待连接对象的创建成功
            countDownLatch.await();
            //会话编号
            System.out.println(zooKeeper.getSessionId());
            zooKeeper.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
