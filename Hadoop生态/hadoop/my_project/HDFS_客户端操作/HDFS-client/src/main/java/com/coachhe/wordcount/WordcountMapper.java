package com.coachhe.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


// map阶段
// KEYIN 第一个参数，为输入数据的key
// VALUEIN 第二个参数，为输入数据的value
// KEYOUT 第三个参数，为输出数据的key的类型， coachhe 3，在这里是coachhe，所以是Text
// VALUEOUT 第四个参数，为输出数据的value，根据上面，是3，所以是LongWritable
public class WordcountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text k = new Text();
    LongWritable v = new LongWritable();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // coachhe coachhe  输入数据

        // 1 获取一行
        String line = value.toString();

        // 2 切割单词
        String[] words = line.split(" ");

        // 3 循环写出
        for (String word : words) {
            k.set(word);
            v.set(1);
            context.write(k, v);
        }

    }
}
