package com.coachhe.windowTest

import com.coachhe.apitest.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath: String = "/Users/heyizhi/Nutstore Files/我的坚果云/程序员/学习/大数据/10 flink/1st flink project/flinkTutorial/src/main/resources/sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)
    val sensorData: DataStream[SensorReading] = inputStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
    // 方法1
    //    val resultStream: KeyedStream[SensorReading, Tuple] = sensorData.keyBy("id")
    // 方法2
    //    val resultStream: KeyedStream[SensorReading, Tuple] = sensorData.keyBy(_.id)
    //    resultStream.print()


    // 实例1
    val resultStream1: WindowedStream[SensorReading, String, TimeWindow] = sensorData
      // lambda表达式，表示所有输入数据（SensorReading类型）的id元素
      .keyBy(_.id)
      .timeWindow(Time.seconds(1))

    // 实例2
    // 目的是每15秒统计一次窗口内所有温度的最小值
    val resultStream2 = sensorData
      .map(data => (data.id, data.temperature))
      // 用二元祖的第一个元素来进行分区
      //      .keyBy(_._1)
      .keyBy(1) //两种都可以
      // .window(TumblingEventTimeWindows.of(Time.seconds(1)))的简写
      .timeWindow(Time.seconds(15))
      // 滑动时间窗口
      //      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))
      // 会话窗口
      //      .window(ProcessingTimeSessionWindows.withGap(Time.minutes(15)))
      .minBy(1)


    //    resultStream2.print()

    val minTempPerWindow: DataStream[(String, Double)] =
      sensorData
        .map(r => (r.id, r.temperature))
        .keyBy(_._1)
        .timeWindow(Time.seconds(15))
        .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))

    //    minTempPerWindow.print()

    val resultStream3: DataStream[(String, List[String])] = env.fromElements(
      ("en", List("tea")), ("fr", List("vin")), ("en", List("cake"))
    ).keyBy(0)
      .reduce((x, y) => (x._1, x._2 ::: y._2))
    resultStream3.print()


    env.execute("Window")
  }
}
