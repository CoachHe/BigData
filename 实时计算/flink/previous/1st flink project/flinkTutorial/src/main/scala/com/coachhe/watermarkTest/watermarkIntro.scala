package com.coachhe.watermarkTest

import com.coachhe.apitest.SensorReading
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark

object watermarkIntro {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 每5秒生成一次水位线
    env.getConfig.setAutoWatermarkInterval(5000)


    val inputStream: DataStream[SensorReading] =
      env.fromElements(SensorReading("sensor1", 100, 20), SensorReading("sensor2", 101, 22))

    inputStream
      // 升序数据提取时间戳
      //.assignAscendingTimestamps(_.timestamp * 1000L)
      // 周期分配器
//      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[SensorReading] {
//        override def getCurrentWatermark: Watermark = ???
//        override def extractTimestamp(t: SensorReading, l: Long): Long = ???
//      })
      // 定点分配器
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[SensorReading] {
        override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = ???
        override def extractTimestamp(t: SensorReading, l: Long): Long = ???
      })
  }

}
