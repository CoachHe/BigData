package com.coachhe.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class wordcount_official {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {   //继承泛型类Mapper
        private final static IntWritable one = new IntWritable(1);  //定义hadoop数据类型IntWritable实例one，并且赋值为1
        private Text word = new Text();                                    //定义hadoop数据类型Text实例word

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException { //实现map函数
            StringTokenizer itr = new StringTokenizer(value.toString());//Java的字符串分解类，默认分隔符“空格”、“制表符(‘\t’)”、“换行符(‘\n’)”、“回车符(‘\r’)”

            while (itr.hasMoreTokens()) {  //循环条件表示返回是否还有分隔符。
                word.set(itr.nextToken());   // nextToken()：返回从当前位置到下一个分隔符的字符串，word.set()：Java数据类型与hadoop数据类型转换
                context.write(word, one);   //hadoop全局类context输出函数write;
            }
        }

    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {    //继承泛型类Reducer
        private IntWritable result = new IntWritable();   //实例化IntWritable

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {  //实现reduce
            int sum = 0;
            for (IntWritable val : values)    //循环values，并记录单词个数
                sum += val.get();
            result.set(sum);   //Java数据类型sum，转换为hadoop数据类型result
            context.write(key, result);   //输出结果到hdfs
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();   //实例化Configuration
/***********
 GenericOptionsParser是hadoop框架中解析命令行参数的基本类。 getRemainingArgs();返回数组
 【一组路径】
 **********/
//函数实现
        /**
         public String[] getRemainingArgs() {
         return (commandLine == null) ? new String[]{} : commandLine.getArgs();
         }
         **/
//总结上面：返回数组【一组路径】
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

//如果只有一个路径，则输出需要有输入路径和输出路径
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");   //实例化job
        job.setJarByClass(wordcount_official.class);   //为了能够找到wordcount这个类
        job.setMapperClass(TokenizerMapper.class);   //指定map类型
/********
 指定CombinerClass类
 这里很多人对CombinerClass不理解
 ************/
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);  //指定reduce类
        job.setOutputKeyClass(Text.class); //rduce输出Key的类型，是Text
        job.setOutputValueClass(IntWritable.class);  // rduce输出Value的类型

        for (int i = 0; i < otherArgs.length - 1; ++i)
            FileInputFormat.addInputPath(job, new Path(String.valueOf(otherArgs)));  //添加输入路径

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));   //添加输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);  //提交job
    }
}