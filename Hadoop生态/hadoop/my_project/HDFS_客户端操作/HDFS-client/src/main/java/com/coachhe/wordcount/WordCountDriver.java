package com.coachhe.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length == 0) {
            args = new String[]{"/Users/heyizhi/BigData/hadoop/my_project/wc_shicao案例操作结果/wc_shicao",
            "/Users/heyizhi/BigData/hadoop/my_project/wc_shicao案例操作结果/wc_output"};
        }

        Configuration conf = new Configuration();
        // 1 获取Job对象
        Job job = Job.getInstance(conf);

        // 2 设置jar存储位置
        job.setJarByClass(WordCountDriver.class);

        // 3 关联Map和Reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 4 设置Mapper阶段输出数据的key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 5 设置最终数据输出的key和value类型
        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(Long.class);

        // 6 设置程序运行的输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 非必要操作：设置分区数量
        job.setNumReduceTasks(2);



        // 7 提交job
        boolean result = job.waitForCompletion(true);//提交完成之后会打印信息,若设置为false则不会打印信息

        System.exit(result ? 0 : 1);
    }
}
