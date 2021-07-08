package com.coachhe.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class ZKWatcherGetData {

    String IP = "10.211.55.100:2181";
    ZooKeeper zooKeeper;

    @Before
    public void before() throws Exception{
        // 计数器
        CountDownLatch countDownLatch = new CountDownLatch(1);
        // arg1:服务器ip和端口
        // arg2:客户端与服务器之间的会话超时时间（ms）
        // arg3:监视器对象
        zooKeeper = new ZooKeeper(IP, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("连接创建成功");
                    // 通知countDownLatch不用继续阻塞了
                    countDownLatch.countDown();
                }
                System.out.println("path = " + event.getPath());
                System.out.println("eventType = " + event.getType());
            }
        });
        //主线程阻塞等待连接对象的创建成功
        countDownLatch.await();
        //会话编号
        System.out.println(zooKeeper.getSessionId());
        zooKeeper.close();
    }

    @After
    public void after() throws Exception{
        zooKeeper.close();
    }

    @Test
    public void watcherGetData1() throws Exception {
        // arg1:节点的路径
        // arg2:使用连接对象中的watcher
        zooKeeper.getData("/watcher2", true, null);
        Thread.sleep(500000);
        System.out.println("结束");
    }

    @Test
    public void watcherGetData2() throws Exception {
        zooKeeper.getData("/watcher2", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("自定义watcher");
                System.out.println("path = " + watchedEvent.getPath());
                System.out.println("eventType = " + watchedEvent.getType());
            }
        }, null);
    }

    @Test
    public void watcherGetData3() throws Exception {

    }

    @Test
    public void watcherGetData4() throws Exception {

    }


}
