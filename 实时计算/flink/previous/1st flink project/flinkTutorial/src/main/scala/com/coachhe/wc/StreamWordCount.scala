package com.coachhe.wc

import org.apache.flink.streaming.api.scala._

// 流处理WordCount
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      // 设置环境
    env.setParallelism(8) // 设置并行度
    

    // 2. 接收一个socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 3. 进行转化处理统计
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 4. 打印输出
    resultDataStream.print()

    // 5. 启动任务执行（调用环境）
    env.execute("stream word count") //job的名字

  }

}
