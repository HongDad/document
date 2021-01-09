package com.nx.flink.streaming.basic.lesson02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  单词计数
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //步骤一：初始化程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //步骤二：数据的输入
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.123.102", 9999);
        //步骤三：数据的处理
        SingleOutputStreamOperator<WordAndOne> result = dataStream.flatMap(
                new FlatMapFunction<String, WordAndOne>() {
                    @Override
                    public void flatMap(String line, Collector<WordAndOne> out) throws Exception {
                        String[] fields = line.split(",");
                        for (String word : fields) {
                            out.collect(new WordAndOne(word, 1));
                        }
                    }
                }
        ).keyBy(tuple -> tuple.word) //flink 1.10.x   1.11
                .sum("one");
        //步骤四：数据的输出
        result.print();
        //步骤五：启动程序
        env.execute("WordCount");

    }


    public static class WordAndOne{
        private String word;
        private Integer one;

        public WordAndOne(){

        }

        public WordAndOne(String word, Integer one) {
            this.word = word;
            this.one = one;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getOne() {
            return one;
        }

        public void setOne(Integer one) {
            this.one = one;
        }

        @Override
        public String toString() {
            return "WordAndOne{" +
                    "word='" + word + '\'' +
                    ", one=" + one +
                    '}';
        }
    }
}