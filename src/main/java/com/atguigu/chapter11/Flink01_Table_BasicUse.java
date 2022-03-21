package com.atguigu.chapter11;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author CoderHyh
 * @create 2022-03-19 19:34
 */
public class Flink01_Table_BasicUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //先创建table的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> streamSource =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //把流转成动态表
        Table table = tableEnv.fromDataStream(streamSource);

        //在动态表上执行查询得到新的动态表
        //Table t1 = table.select("id,ts.vc");
        Table t1 = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts").as("tt"), $("vc"));

        //把查询结果的动态表转成流写出去
        DataStream<Row> result = tableEnv.toAppendStream(t1, Row.class);
        result.print();
        env.execute();

        //t1.execute().print();


    }
}
