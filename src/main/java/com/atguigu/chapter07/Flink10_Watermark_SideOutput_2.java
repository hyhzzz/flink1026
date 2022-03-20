package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author CoderHyh
 * @create 2022-03-19 8:26
 */
public class Flink10_Watermark_SideOutput_2 {
    public static void main(String[] args) throws Exception {

        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<WaterSensor> outputTags2 = new OutputTag<WaterSensor>("sensor_2") {
        };

        SingleOutputStreamOperator<String> main = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.parseLong(data[1]) * 1000, Integer.valueOf(data[2]));
                })
                //分配水印
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            //返回这套数据的事件时间，必须是毫秒
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {
                                return waterSensor.getTs();
                            }
                        }))

                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context,
                                        Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                        if ("sensor_1".equalsIgnoreCase(key)) {
                            for (WaterSensor ele : iterable) {
                                collector.collect(ele.toString());
                            }
                        } else if ("sensor_2".equalsIgnoreCase(key)) {
                            for (WaterSensor ele : iterable) {
                                context.output(outputTags2, ele);
                            }
                        }
                    }
                });

        main.print();
        main.getSideOutput(outputTags2).print();

        //启动执行环境
        env.execute();
    }
}
