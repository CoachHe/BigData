package com.coachhe.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient2 {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration cf = new Configuration();

        // 1 获取hdfs客户端对象
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), cf, "coachhe");

        // 2 在hdfs上创建路径
        fs.mkdirs(new Path("/0930/dashen/coachhe"));

        // 3 关闭连接
        fs.close();

        System.out.println("over");
    }
}
