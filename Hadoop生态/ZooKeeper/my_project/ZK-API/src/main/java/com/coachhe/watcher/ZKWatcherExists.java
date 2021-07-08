package com.coachhe.watcher;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class ZKWatcherExists {

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

    // 使用连接对象的监视器
    @Test
    public void watcherExists1() throws Exception {
        // arg1表示节点的路径
        // true表示需要进行监听,连接对象中的watcher
        zooKeeper.exists("/watcher1", true);
        Thread.sleep(50000);
        System.out.println("结束");
    }

    // 自定义监视器
    @Test
    public void watcherExists2() throws Exception {
        zooKeeper.exists("/watcher1", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("自定义watcher");
                System.out.println("path = " + watchedEvent.getPath());
                System.out.println("eventType = " + watchedEvent.getType());
            }
        });
        Thread.sleep(50000);
        System.out.println("结束");
    }

    // watcher一次性
    @Test
    public void watcherExists3() throws Exception {
        zooKeeper.exists("/watcher1", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    System.out.println("自定义watcher");
                    System.out.println("path = " + watchedEvent.getPath());
                    System.out.println("eventType = " + watchedEvent.getType());
                    zooKeeper.exists("/wathcer1", this);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        Thread.sleep(50000);
        System.out.println("结束");
    }

    // 注册多个监听器对象
    @Test
    public void watcherExists4() throws Exception {
        zooKeeper.exists("/watcher1", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("1");
            }
        });
        zooKeeper.exists("/watcher1", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("2");
            }
        });
    }

}
