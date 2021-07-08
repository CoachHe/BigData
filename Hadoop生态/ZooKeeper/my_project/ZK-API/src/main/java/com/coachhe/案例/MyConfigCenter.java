package com.coachhe.案例;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class MyConfigCenter implements Watcher {
    String IP = "10.211.55.100:2181";
    static ZooKeeper zooKeeper;
    CountDownLatch countDownLatch = new CountDownLatch(1);

    // 用于本地化存储配置信息
    private String url;
    private String username;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    private String password;

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
        } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            initValue();
        }
    }

    // 连接zookeeper服务器，读取配置信息
    public void initValue(){
        try {
            // 创建连接对象
            zooKeeper = new ZooKeeper(IP, 5000, this);
            // 阻塞线程，等待连接创建成功
            countDownLatch.await();
            // 读取配置信息
            this.url = new String(zooKeeper.getData("/config/url", true, null));
            this.username = new String(zooKeeper.getData("/config/username", true, null));
            this.password = new String(zooKeeper.getData("/config/password", true, null));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    // 构造方法
    public MyConfigCenter() {
        initValue();
    }


    public static void main(String[] args) {
        try {
            MyConfigCenter myConfigCenter = new MyConfigCenter();
            for (int i = 1; i <= 10; i++) {
                Thread.sleep(3000);
                System.out.println("url:" + myConfigCenter.getUrl());
                System.out.println("username:" + myConfigCenter.getUsername());
                System.out.println("password:" + myConfigCenter.getPassword());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
