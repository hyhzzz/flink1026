package com.atguigu.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author CoderHyh
 * @create 2022-03-18 17:12
 */
public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink04_Source_Kafka");
        //消费的时候如果没有上次的消费记录则从最新位置开始消费，如果有上次的消费记录则从上次的位置开始消费
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        kafkaSource.print();

        env.execute();
    }
}
