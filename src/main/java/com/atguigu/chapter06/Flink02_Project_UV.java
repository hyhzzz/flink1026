package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @author CoderHyh
 * @create 2022-03-19 0:42
 */
public class Flink02_Project_UV {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> stream = env.readTextFile("input/UserBehavior.csv");

        stream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] data = line.split(",");
                        UserBehavior ub = new UserBehavior(
                                Long.valueOf(data[0]),
                                Long.valueOf(data[1]),
                                Integer.valueOf(data[2]),
                                data[3],
                                Long.valueOf(data[4]));
                        if ("pv".equalsIgnoreCase(ub.getBehavior())) {
                            collector.collect(Tuple2.of("uv", ub.getUserId()));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .process(
                        new KeyedProcessFunction<String, Tuple2<String, Long>, Long>() {
                            Set<Long> uids = new HashSet<>();

                            @Override
                            public void processElement(Tuple2<String, Long> stringLongTuple2, KeyedProcessFunction<String, Tuple2<String, Long>, Long>.Context context, Collector<Long> collector) throws Exception {
                                uids.add(stringLongTuple2.f1);
                                collector.collect((long) uids.size());
                            }
                        }
                ).print();

        env.execute();
    }
}
