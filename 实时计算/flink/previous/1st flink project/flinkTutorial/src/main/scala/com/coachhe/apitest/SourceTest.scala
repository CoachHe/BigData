package com.coachhe.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

// 定义样例类
case class SensorReading(id : String, timestamp : Long, temperature : Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从集合中读取数据
    val dataList = List(
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.7),
        SensorReading("sensor_10", 1547718205, 38.1)
    )
    val stream1 = env.fromCollection(dataList)
//    stream1.print()

    // 2. 从文件中读取数据
    val inputPath = "/Users/heyizhi/Nutstore Files/我的坚果云/程序员/学习/大数据/10 flink/1st flink project/flinkTutorial/src/main/resources/sensor.txt"
    val stream2 = env.readTextFile(inputPath)
//    stream2.print()

    // 3. 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//    stream3.print()

    // 4. 自定义Source
    val stream4: DataStream[SensorReading] = env.addSource(new MySensorSource())
    stream4.print()

    env.execute("source test")

  }

}

// 自定义SourceFunction
class MySensorSource() extends SourceFunction[SensorReading]{
  // 定义一个随机数发生器
  val rand = new Random()
  // 随机生成一组10个传感器的初始温度:(id,temp)
  var curTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))
  // 定义一个标志位flag，用来表示数据源是否正确发出数据
  var running : Boolean = true
  override def cancel():Unit = running = false
  override def run(ctx: SourceFunction.SourceContext[SensorReading]) : Unit = {
    // 定义无限循环，不停产生数据，除非被cancel
    while(running){
      // 在上次数据基础上微调更新温度值
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取当前时间戳加入到数据中
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        data => ctx.collect(SensorReading(data._1, curTime, data._2))
      )
      // 间隔100ms
      Thread.sleep(100)
    }
  }
}