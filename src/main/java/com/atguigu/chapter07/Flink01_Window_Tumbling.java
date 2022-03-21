package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

/**
 * @author CoderHyh
 * @create 2022-03-19 8:26
 */
public class Flink01_Window_Tumbling {
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
                //Tuple2<String, Long：数据输入的类型  String:输出 String：key TimeWindow：窗口类型
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,  //窗口属于的key的值
                                        ProcessWindowFunction<Tuple2<String, Long>,
                                                String, String, TimeWindow>.Context context,//上下文对象 获取到窗口的开始时间和结束时间
                                        Iterable<Tuple2<String, Long>> iterable, //这个窗口的所有数据
                                        Collector<String> collector) throws Exception {
                        Date start = new Date(context.window().getStart());
                        Date end = new Date(context.window().getEnd());

                        ArrayList<String> strings = new ArrayList<>();
                        for (Tuple2<String, Long> ele : iterable) {
                            strings.add(ele.f0);
                        }
                        collector.collect("key=" + key + "start=" + start + "end=" + end+"data="+strings);

                    }
                })
                .print();

        //启动执行环境
        env.execute();
    }
}
