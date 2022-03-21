package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-18 18:07
 */
public class Flink07_Transform_Map {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);

        //stream.map(
        //        new MapFunction<Integer, Integer>() {
        //            @Override
        //            public Integer map(Integer integer) throws Exception {
        //                return  integer * integer;
        //            }
        //        }
        //).print();

        stream.map(new RichMapFunction<Integer, Integer>() {
            //执行的次数和并行度一样的，和算子的并行度一致
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open生命周期...");
                //建立连接对象
            }

            @Override
            public Integer map(Integer integer) throws Exception {
                return  integer * integer;
            }

            @Override
            public void close() throws Exception {
                System.out.println("close生命周期...");
                //关闭资源
            }
        }).print();

        env.execute();
    }

}