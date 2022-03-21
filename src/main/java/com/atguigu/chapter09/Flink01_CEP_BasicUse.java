package com.atguigu.chapter09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author CoderHyh
 * @create 2022-03-19 17:31
 */
public class Flink01_CEP_BasicUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0],
                                Long.parseLong(split[1]) * 1000,
                                Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        //定义模式
        //<WaterSensor>：表示你把什么数据输入到这个模式中 "start":模式的名字  begin:开始模式  给模式起名字
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start")
                //where条件：SimpleCondition简单条件
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        //true暴露 false 丢弃
                        return "sensor_1".equalsIgnoreCase(value.getId());
                    }
                });

        //把模式运用在流上
        PatternStream<WaterSensor> ps = CEP.pattern(waterSensorStream, pattern);

        //获取匹配到的结果
        ps.select(new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print();

        env.execute();
    }
}
