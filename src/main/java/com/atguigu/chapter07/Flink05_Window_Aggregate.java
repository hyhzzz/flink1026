package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-19 8:26
 */
public class Flink05_Window_Aggregate {
    public static void main(String[] args) throws Exception {

        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        for (String word : s.split(" ")) {
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        System.out.println("createAccumulator");
                        return 0L;
                    }

                    @Override
                    public Long add(Tuple2<String, Long> stringLongTuple2, Long aLong) {
                        System.out.println("add");
                        return aLong + stringLongTuple2.f1;
                    }

                    @Override
                    public Long getResult(Long aLong) {
                        System.out.println("getResult");
                        return aLong;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        System.out.println("merge");
                        return aLong + acc1;
                    }
                }).print();

        //启动执行环境
        env.execute();
    }
}
