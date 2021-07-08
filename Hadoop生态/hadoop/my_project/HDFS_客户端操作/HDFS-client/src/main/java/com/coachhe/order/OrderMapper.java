package com.coachhe.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 0000001 Pdt_01 222.8 输入数据
        // 将0000001和222.8封装到bean对象之后输出就行了，不需要value
        String[] line = value.toString().split(" ");
        k.setOrder_id(Integer.parseInt(line[0]));
        k.setPrice(Double.parseDouble(line[2]));
        context.write(k, NullWritable.get());
    }
}
