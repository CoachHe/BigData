package com.coachhe.apitest

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

//case class SensorReading(id:String, timestamp:Long, temperature:Double)
case class Address(
                  address: String,
                  zip: String,
                  country: String
                  )
case class Person(
                 name: String,
                 birthday: (Int, Int, Int), //年月日
                 address: Address
                 )

object KeyValueTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[(Int, String, Double)] = env.fromElements((1, "Hello", 1.1), (2, "World", 2.2))
      .map(a => (a._1 + 1, a._2 + "1", a._3 + 1))
    val keyed: KeyedStream[(Int, String, Double), Tuple] = input.keyBy(1)
    //    keyed.print()
    //    env.execute("tuple_key")

    val sensorStream: DataStream[SensorReading] = env.fromElements(SensorReading("1", 20200225, 20.0), SensorReading("2", 20200226, 21.0))
//    val keyedSensors: KeyedStream[SensorReading, Tuple] = sensorStream.keyBy("id")
    sensorStream.print()
//    keyedSensors.print()
    env.execute("keyedSensors")

    val POJOStream: DataStream[Person_POJO] = env.fromElements(new Person_POJO("zhangsan", 1), new Person_POJO("lisi", 2))
    val POJOSensors: KeyedStream[Person_POJO, Tuple] = POJOStream.keyBy("age")
    //    POJOSensors.print()
    //    env.execute("POJO")

    // 用"."来进行选择
    val a_person: DataStream[Person] = env.fromElements(new Person("zhangsan", (1, 1, 1), new Address("a", "b", "c")))
    val persons: DataStream[Person] = a_person.keyBy("address.zip")
//        persons.print()
//        env.execute(".")

    // 键值选择器
    val byId: KeyedStream[SensorReading, String] = sensorStream
      .keyBy(r => r.id)

    // 键值选择器2
    val key_input: DataStream[(Int, Int)] = env.fromElements((1, 1), (2, 3))
    val keyedStream = key_input.keyBy(value => math.max(value._1, value._2))
//    keyedStream.print()
//    env.execute("key_selector")

  }

}
