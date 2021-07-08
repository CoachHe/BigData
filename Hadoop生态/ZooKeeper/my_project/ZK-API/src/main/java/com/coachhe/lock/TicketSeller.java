package com.coachhe.lock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class TicketSeller {

    private void sell(){
        System.out.println("开始售票");
        //线程随机休眠数毫秒，模拟现实中的费时操作
        int sleepMillis = 5000;
        try {
            //代表复杂逻辑执行了一段时间
            Thread.sleep(sleepMillis);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("售票结束");
    }

    public void sellTicketWithLock() throws Exception{
        MyLock lock = new MyLock();
        // 获取锁
        lock.acquireLock();
        sell();
        // 释放锁
        lock.releaseLock();
    }

    public static void main(String[] args) throws Exception {
        TicketSeller ticketSeller = new TicketSeller();
//        ExecutorService service = Executors.newCachedThreadPool();
//        for (int i = 0; i < 20; i++) {
//            service.execute(() -> {
//                try {
//                    ticketSeller.sellTicketWithLock();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            });
//        }
        for (int i = 0; i < 10; i++) {
            ticketSeller.sellTicketWithLock();
        }
    }
}
