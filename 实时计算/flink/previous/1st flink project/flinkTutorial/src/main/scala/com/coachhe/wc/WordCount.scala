package com.coachhe.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

// 批处理wordcount
object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2. 从文件中读取数据
    val inputPath: String = "/Users/heyizhi/Nutstore Files/我的坚果云/程序员/学习/大数据/10 flink/1st flink project/flinkTutorial/src/main/resources/hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    // 3. 对数据进行转换处理统计，先分词，再按照word进行分组，最后进行聚合统计
    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) // 以第一个元素作为key，进行分组
      .sum(1) // 对当前分组的所有数据的第2个元素求和


    // 4. 打印输出
    resultDataSet.print()
  }

}
