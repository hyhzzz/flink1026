package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-18 18:07
 */
public class Flink16_Transform_Process_KeyBy_Post {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 10L, 20),
                new WaterSensor("sensor_2", 30L, 50),
                new WaterSensor("sensor_1", 40L, 10),
                new WaterSensor("sensor_2", 100L, 30),
                new WaterSensor("sensor_2", 3L, 80),
                new WaterSensor("sensor_1", 200L, 100));


        stream.keyBy(WaterSensor::getId)
                //第一个泛型表示：key的类型  第二个：输入的数据类型  第三个：输出的数据类型
                .process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
                    int sum = 0;
                    @Override
                    public void processElement(WaterSensor value,
                                               KeyedProcessFunction<String, WaterSensor, Integer>.Context context,
                                               Collector<Integer> collector) throws Exception {


                        sum += value.getVc();
                        collector.collect(sum);
                    }
                }).print();
        env.execute();
    }

}