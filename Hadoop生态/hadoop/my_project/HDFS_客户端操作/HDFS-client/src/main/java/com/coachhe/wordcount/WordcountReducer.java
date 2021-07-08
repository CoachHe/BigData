package com.coachhe.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Reduce阶段
// KEYIN 第一个参数，为输入数据的key
// VALUEIN 第二个参数，为输入数据的value
// KEYOUT 第三个参数，为输出数据的key的类型， coachhe 3，在这里是coachhe，所以是Text
// VALUEOUT 第四个参数，为输出数据的value，根据上面，是3，所以是LongWritable
public class WordcountReducer extends Reducer<Text, LongWritable, String, Long> {
    // 输入为 coachhe 1 的形式，key为Text， value为LongWritable
    // 输出为 coachhe 5 的形式，key为String，value为Integer


    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;

        // 1 累加求和
        for (LongWritable value : values) {
            sum += value.get();
        }

        // 2 写出
        context.write(key.toString(), sum);

    }
}
