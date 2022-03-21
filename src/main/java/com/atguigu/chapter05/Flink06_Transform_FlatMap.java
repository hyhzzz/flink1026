package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-18 18:07
 */
public class Flink06_Transform_FlatMap {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);

        stream.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            //value:就是我们传进来的数据
            public void flatMap(Integer value, Collector<Integer> collector) throws Exception {
                //collector.collect(value * value);
                //collector.collect(value * value * value);
                if (value % 2 == 0) {
                    collector.collect(value);
                }
            }
        }).print();

        env.execute();
    }

}