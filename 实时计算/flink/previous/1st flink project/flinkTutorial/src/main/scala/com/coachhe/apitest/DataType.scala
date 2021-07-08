package com.coachhe.apitest


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala.typeutils.Types

object DataType {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 原始类型
    val nums: DataStream[Long] = env.fromElements(1L, 2L, 3L, 4L, 5L)
    nums.map(n => n + 1)
//    nums.print()
//    env.execute("basic type")

    // Java和Scala元祖
    // Scala元祖
    val persons: DataStream[(String, Int)] = env.fromElements(
      ("Adam", 17),
      ("Sarah", 23)
    )
    persons.filter(p => p._2 > 18)
//    persons.print()
//    env.execute("Tuple")


    // Scala样例类
    val personsss: DataStream[Person] = env.fromElements(
      Person("Adam", 17),
      Person("Sarah", 23)
    )
    // 过滤出那些年龄大于18的人
    personsss.filter( p => p.age > 18)
//    personsss.print()
//    env.execute("Scala case class")

    // POJO
    val personssss: DataStream[Person_POJO] = env.fromElements(
      new Person_POJO("Alex", 42),
      new Person_POJO("Wendy", 23)
    )
    personssss.filter( p => p.age > 18)
    personsss.print()
    env.execute("POJO")


    // TypeInformation
    // 1. 原始类型的TypeInformation
    val stringType: TypeInformation[String] = Types.STRING
    // 2. Scala元祖的TypeInformation
    val tupleType: TypeInformation[(Int, Long)] = Types.TUPLE[(Int, Long)]
    // 3. 样例类的TypeInformation
    val caseClassType: TypeInformation[Person] = Types.CASE_CLASS[Person]



  }
  case class Person(name: String, age: Int)

}
