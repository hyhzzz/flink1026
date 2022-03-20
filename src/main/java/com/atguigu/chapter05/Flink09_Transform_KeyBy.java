package com.atguigu.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-18 18:07
 */
public class Flink09_Transform_KeyBy {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);

        stream.keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer value) throws Exception {
                return value %2 == 0 ?"偶数":"奇数" ;
            }
        }).print();


        env.execute();
    }

}