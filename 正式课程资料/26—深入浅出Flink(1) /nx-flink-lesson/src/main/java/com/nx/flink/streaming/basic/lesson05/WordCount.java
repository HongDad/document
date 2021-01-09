package com.nx.flink.streaming.basic.lesson05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  单词计数
 *  
 *  hello,1
 *  hello,1
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //步骤一：初始化程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
      //  env.setParallelism(2);

        //步骤二：数据的输入
        DataStreamSource<String> dataStream =
                env.socketTextStream("192.168.123.102", 9999).setParallelism(1); //1 task

        //步骤三：数据的处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result =
                dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line,
                                Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    //  out.collect(new Tuple2<String,Integer>(word,1));
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }) .setParallelism(2) //2 task
                        .keyBy(tuple -> tuple.f0)
                .sum(1).setParallelism(2); //2 task
        //步骤四：数据的输出
        result.print().setParallelism(2);

        //这个任务运行起来应该是几个task?
        //步骤五：启动程序
        env.execute("WordCount");

    }
}
