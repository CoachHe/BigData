package com.coachhe.windowTest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object TimeWindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置事件时间
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  }

}
