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
public class Flink02_CEP_Loop {
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
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equalsIgnoreCase(value.getId());
                    }
                })
                //.times(2); //表示前面这个模式出现两次才匹配  出现两次的放在一个集合
                //.times(2,3);
                //.oneOrMore();
                .timesOrMore(2);

        //把模式运用在流上
        PatternStream<WaterSensor> waterSensorPatternStream = CEP.pattern(waterSensorStream, pattern);

        //获取匹配到的结果
        waterSensorPatternStream.select(new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print();

        env.execute();
    }
}
