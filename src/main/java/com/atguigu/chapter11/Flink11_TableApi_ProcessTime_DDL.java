package com.atguigu.chapter11;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-20 19:10
 */
public class Flink11_TableApi_ProcessTime_DDL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建表, 声明一个额外的列作为处理时间
        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int,pt_time as PROCTIME())" +
                " with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor.txt',"
                + "'format' = 'csv'"
                + ")");

        TableResult result = tableEnv.executeSql("select * from sensor");

        result.print();

    }
}
