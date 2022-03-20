package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-18 16:54
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        //DataStreamSource<Integer> stream = env.fromCollection(list);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 34, 5, 56);
        stream.print();

        env.execute();
    }
}