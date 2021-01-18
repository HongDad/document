package com.nx.flink.streaming.basic.lesson06;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic="test"; //3
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers","192.168.123.102:9092");
        consumerProperties.setProperty("group.id","testSlot_consumer");


        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(topic,
                new SimpleStringSchema(),
                consumerProperties);
        /**
         *步骤一：数据的输入
         * topic -> partition -> 并行度
         */
        DataStreamSource<String> data = env.addSource(myConsumer).setParallelism(3);// 3 task


        //flatmap /map /filter
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOneStream = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            public void flatMap(String line,
                                Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).setParallelism(2); //2 task

        //分组聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordOneStream
                .keyBy(value -> value.f0)
                .sum(1)
                .setParallelism(2);//2 task

        result.map( tuple -> tuple.toString()).setParallelism(2) // 2 task
                .print().setParallelism(1); //1 task

        env.execute("WordCount2");

    }

}
