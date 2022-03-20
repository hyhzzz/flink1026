package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-18 9:40
 * 流处理--》有界流
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {

        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据：无界流和有界流
        DataStreamSource<String> lineStream = env.readTextFile("input/words.txt");

        // 各种转换
        //flatMap
        //输入      切完之后输出什么
        //<String, Tuple2<String, Long>
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneStream = lineStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //keyby
        //KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordAndOneStream.keyBy(0);
        //<Tuple2<String, Long>：流中的数据类型    , String：key的类型
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndOneStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        //sum
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        //打印结果
        result.print();

        //执行流环境
        env.execute();
    }
}
