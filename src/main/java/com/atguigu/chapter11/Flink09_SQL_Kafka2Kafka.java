package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-20 18:46
 */
public class Flink09_SQL_Kafka2Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 注册SourceTable: source_sensor
        tableEnv.executeSql("create table source_sensor (" +
                "id string, " +
                "ts bigint, " +
                "vc int) " +
                "with("
                //连接的名字
                + "'connector' = 'kafka',"
                //消费者的topic
                + "'topic' = 'topic_source_sensor',"
                //kafka地址
                + "'properties.bootstrap.servers' = 'hadoop102:9029,hadoop103:9092,hadoop104:9092',"
                //消费者组
                + "'properties.group.id' = 'atguigu',"
                //扫描模式 offset
                + "'scan.startup.mode' = 'latest-offset',"
                //kafka的格式
                + "'format' = 'json'"
                + ")");

        // 2. 注册SinkTable: sink_sensor
        tableEnv.executeSql("create table sink_sensor(" +
                "id string," +
                " ts bigint," +
                " vc int) " +
                "with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sink_sensor',"
                + "'properties.bootstrap.servers' = 'hadoop102:9029,hadoop103:9092,hadoop104:9092',"
                + "'format' = 'json'"
                + ")");

        // 3. 从SourceTable 查询数据, 并写入到 SinkTable
        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1'");
    }
}
