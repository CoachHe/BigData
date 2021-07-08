package com.coachhe.windowTest

import com.coachhe.apitest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class MinMaxTemp(id:String, min:Double, max:Double, endTs:Long)

/**
 * 该ProcessWindowFunction用于计算每个
 * 窗口内的最低和最高温度读数，
 * 它会将读数连同窗口结束时间戳一起发起。
 */
class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading, MinMaxTemp , String, TimeWindow]{
  override def process(key: String,
                       ctx: Context,
                       vals: Iterable[SensorReading],
                       out: Collector[MinMaxTemp]): Unit = {
    val temps: Iterable[Double] = vals.map(_.temperature) // 通过map将Iterator里面的所有数据转换成Double
    val windowEnd: Long = ctx.window.getEnd
    out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
  }
}

object WindowFunctions {


  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    val sensorData: DataStream[SensorReading] = {
      env.fromElements(SensorReading("sensor1", 100, 20), SensorReading("sensor2", 101, 21))
    }


    // output the lowest and highest temperature reading every 5 seconds

    val minMaxTempPerWindow: DataStream[MinMaxTemp] = sensorData
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new HighAndLowTempProcessFunction)

    // print result stream
    minMaxTempPerWindow.print()

    env.execute()
  }
}
