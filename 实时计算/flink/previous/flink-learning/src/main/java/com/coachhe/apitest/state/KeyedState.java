package com.coachhe.apitest.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.SensorReading;


public class KeyedState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前sensor数据个数
        DataStream<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        resultStream.print();


        env.execute();
    }

    //自定义MapFunction，普通MapFunction不能完成，必须使用RichMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer>{

        private ValueState<Integer> keyCountState;
        private ListState<String> listState;
        private MapState<String, Integer> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("my-map", String.class, Integer.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            // 值状态API调用
            Integer count = keyCountState.value();
            if (count == null) {
                count = 0;
            }
            count++;
            keyCountState.update(count);

            // 列表状态API调用
            Iterable<String> strings = listState.get();
            // 每次增加一行，值为count
            listState.add(String.valueOf(count));
            // 然后遍历
            for (String each : strings) {
                System.out.println("the element of strings is " + each);
            }

            // map state
            mapState.get("1");
            mapState.put("2", 1);
            mapState.remove("2");

            return count;
        }
    }



}
