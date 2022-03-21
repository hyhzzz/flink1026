package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author CoderHyh
 * @create 2022-03-20 18:41
 */
public class Flink08_SQL_BaseUse_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        // 使用sql查询一个已注册的表
        // 1. 从流得到一个表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        // 2. 把注册为一个临时视图
        tableEnv.createTemporaryView("sensor", table);

        //没办法拿到table对象
        //tableEnv.createTemporaryView("sensor",waterSensorStream);

        // 3. 在临时视图查询数据, 并得到一个新表
        Table result = tableEnv.sqlQuery("select * from sensor where id='sensor_1'");

        // 4. 显示resultTable的数据
        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();


    }
}
