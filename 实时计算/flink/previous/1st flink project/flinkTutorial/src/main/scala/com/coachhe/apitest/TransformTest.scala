package com.coachhe.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 0.读取数据
    val inputPath = "/Users/heyizhi/Nutstore Files/我的坚果云/程序员/学习/大数据/10 flink/1st flink project/flinkTutorial/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    // 1.转换成样例类（简单转换操作）
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 2.分组聚合，输出每个传感器当前温度最小值
    val aggStream = dataStream
      .keyBy(0)
      .min("temperature")
//    aggStream.print()

    // 3.需要输出当前最小的温度值，以及最近的时间戳，要用reduce
    val resultStream = dataStream
      .keyBy("id")
      .reduce((curState, newData) =>
        SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature))
      )
//    resultStream.print()

    // 4.1 分流操作(split和select)
    val splitStream = dataStream
      .split(
        data => {
          if (data.temperature > 30.0) Seq("high") else Seq("low")
        }
      )
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")


//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")

    // 4.2 合流操作
    val warningStream = highTempStream
      .map(data => (data.id, data.temperature))

    // 语法为：first.connect(second)
    val  connectedStreams = warningStream.connect(lowTempStream)

    // 用coMap对数据进行分别处理
    val coMapResultStream = connectedStreams
      .map(
        warningData => (warningData._1, warningData._2, "warning"),
        lowTempData => (lowTempData.id, "healthy")
      )

    val numbers: DataStream[Long] = env.fromElements(1L, 2L, 3L, 4L)
    numbers.map(n => n + 1)

//    coMapResultStream.print("coMap")

    env.execute("transform test")
  }
}

// 自定义一个函数类
class MyFilter extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = t.id.startsWith("sensor_1")
}

// 富函数
class MyRichFilter extends RichFilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = ???

  override def open(parameters: Configuration): Unit = {
    //做一些初始化操作，比如数据库的连接
  }

  override def close(): Unit = {
    // 一般做收尾工作，比如关闭连接，或者清空状态
  }
}
