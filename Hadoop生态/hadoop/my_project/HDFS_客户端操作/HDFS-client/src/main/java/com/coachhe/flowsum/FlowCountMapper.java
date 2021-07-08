package com.coachhe.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 7 138xxxxxxxx 120.178.231.32 1116 954 200 输入数据类型

        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] fieles = line.split(" ");

        // 3 封装对象
        k.set(fieles[1]);  //封装手机号
        v.setUpFlow(Long.parseLong(fieles[fieles.length - 3]));
        v.setDownFlow(Long.parseLong(fieles[fieles.length - 2]));

        // 4 写出
        context.write(k,v);

    }
}
