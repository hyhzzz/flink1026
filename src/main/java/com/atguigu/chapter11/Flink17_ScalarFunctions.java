package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author CoderHyh
 * @create 2022-03-20 20:35
 */
public class Flink17_ScalarFunctions {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //4.不注册函数直接使用
//        table.select(call(Mylenth.class,$("id"))).execute().print();

        //4.1先注册再使用
        tableEnv.createTemporarySystemFunction("MyLenth", Mylenth.class);

        //TableAPI
//        table.select(call("MyLenth", $("id"))).execute().print();

        //SQL
        tableEnv.executeSql("select MyLenth(id) from " + table).print();

    }

    //自定义UDF函数，求数据的长度
    public static class Mylenth extends ScalarFunction {
        public int eval(String value) {
            return value.length();
        }
    }

}
