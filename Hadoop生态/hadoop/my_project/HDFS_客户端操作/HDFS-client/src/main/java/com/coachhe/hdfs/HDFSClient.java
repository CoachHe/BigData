package com.coachhe.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop100:9000");
        // 1 获取hdfs客户端对象
        FileSystem fs = FileSystem.get(conf);
        // 2 在hdfs上创建路径
        fs.mkdirs(new Path("/0930/dashen"));
        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }

    //上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {

        // 1. 获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "coachhe");

        // 2. 执行上传API
        fs.copyFromLocalFile(new Path("/Users/heyizhi/Downloads/coachhe"), new Path("/coachhe_re1"));

        // 3. 关闭资源
        fs.close();
    }

    //下载
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "coachhe");

        // 2. 执行上传API
        // fs.copyToLocalFile(new Path("/coachhe_re1"), new Path("/Users/heyizhi/Downloads"));
        // 可以选择剪切操作，是否删除元数据
        fs.copyToLocalFile(false, new Path("/coachhe_re1"), new Path("/Users/heyizhi/Downloads/coachhe2"),
                true);

        // 3. 关闭资源
        fs.close();

    }


    // 文件删除
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {

        // 1. 获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "coachhe");

        // 2. 文件删除
        fs.delete(new Path("/0930"), true);

        // 3. 关闭资源
        fs.close();

    }

    //HDFS文件名称修改
    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), configuration, "atguigu");

        // 2 修改文件名称
        fs.rename(new Path("/banzhang.txt"), new Path("/banhua.txt"));

        // 3 关闭资源
        fs.close();
    }

    //HDFS文件详情查看
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), configuration, "coachhe");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-----------班长的分割线----------");
        }
        // 3 关闭资源
        fs.close();
    }

    //HDFS文件和文件夹判断
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), configuration, "coachhe");
        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        // 3 关闭资源
        fs.close();
    }

    //IO流实现文件上传
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), configuration, "coachhe");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("Downloads/banhua.txt"));
        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/banhua.txt"));
        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 文件下载
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), configuration, "coachhe");
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/banhua.txt"));
        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("Downloads/banhua.txt"));
        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }




}
