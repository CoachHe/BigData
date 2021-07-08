package com.coachhe.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZKCreate {
    String IP = "10.211.55.100:2181";
    ZooKeeper zooKeeper;

    @Before
    public void before() throws Exception{
        // 计数器对象
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
    public void create1() throws Exception{
        // 同步创建节点
        // arg1:节点的路径
        // arg2:节点的数据
        // arg3:权限列表 world:anyone:cdrwa
        // arg4:节点类型 持久化
        zooKeeper.create("/create/node1", "node1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void create2() throws Exception{
        // arg3是 world:anyone:r
        zooKeeper.create("/create/node2", "node2".getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    @Test
    public void create3() throws Exception{
        // world授权模式
        // 权限列表
        List<ACL> acls = new ArrayList<>();
        // 授权模式和授权对象
        Id id = new Id("world", "anyone");
        // 权限设置
        acls.add(new ACL(ZooDefs.Perms.READ, id));
        acls.add(new ACL(ZooDefs.Perms.WRITE, id));
    }

    @Test
    public void create4() throws Exception{
        // ip 授权模式
        List<ACL> acls = new ArrayList<>();
        Id id = new Id("ip", "10.211.55.100");
        acls.add(new ACL(ZooDefs.Perms.ALL, id));
        zooKeeper.create("/create/node4", "node4".getBytes(), acls, CreateMode.PERSISTENT);
    }

    @Test
    public void create5() throws Exception{
        // auth 授权模式
        // 添加授权用户
        zooKeeper.addAuthInfo("digest", "itcast:123456".getBytes());
        zooKeeper.create("/create/node5", "node5".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL,
                CreateMode.PERSISTENT);
    }


}
