package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-18 18:07
 */
public class Flink14_Transform_Reduce {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 10L, 20),
                new WaterSensor("sensor_2", 30L, 50),
                new WaterSensor("sensor_1", 40L, 10),
                new WaterSensor("sensor_2", 100L, 30),
                new WaterSensor("sensor_2", 3L, 80),
                new WaterSensor("sensor_1", 200L, 100));

        stream.keyBy(WaterSensor::getId)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    //WaterSensor value1, WaterSensor value2  value1：第一个是上次一次的聚合结果，value2：这一次参与聚合的数据
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
                    }
                }).print();


        env.execute();
    }

}