package com.coachhe.windowTest

import java.util.Collections

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object DIY_WindowFunction {
  def main(args: Array[String]): Unit = {

  }

}

class MyWindowAssigner extends WindowAssigner[Object, TimeWindow] {

  val windowSize: Long = 30 * 1000L

  override def assignWindows(
                              o: Object,
                              ts: Long,
                              ctx: WindowAssigner.WindowAssignerContext): java.util.List[TimeWindow] =
    {
      // 30s取余
      val startTime: Long = ts - (ts % windowSize)
      val endTime: Long = startTime + windowSize
      // 发出相应的时间窗口
      Collections.singletonList(new TimeWindow(startTime, endTime))
    }

  override def getDefaultTrigger(
                                  env: StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
    EventTimeTrigger.create()
  }

  override def getWindowSerializer(
                                    executionConfig: ExecutionConfig):
  TypeSerializer[TimeWindow] = {
    new TimeWindow.Serializer
  }

  override def isEventTime: Boolean = true
}
