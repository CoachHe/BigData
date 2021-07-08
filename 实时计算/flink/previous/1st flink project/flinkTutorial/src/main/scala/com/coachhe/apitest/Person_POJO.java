package com.coachhe.apitest;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import scala.Tuple2;


public class Person_POJO {
    public String name;
    public int age;

    public Person_POJO(){}

    public Person_POJO(String name, int age) {
        this.name = name;
        this.age = age;
    }

    // TypeInformation
    // 原始类型的TypeInformation
//    TypeInformation<Integer> intType = Types.INT;
    // Java元祖的TypeInformation
    //TypeInformation<Tuple2<Long,String>> tupleType = Types.TUPLE(Types.LONG, Types.STRING);
    // POJO的TypeInformation
//    TypeInformation<Person_POJO> personType = Types.POJO(Person_POJO.class);


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
