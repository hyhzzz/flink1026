package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author CoderHyh
 * @create 2022-03-18 18:07
 */
public class Flink21_Sink_Custom {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                })
                .addSink(new MySqlSink());


        env.execute();
    }

    public static class MySqlSink extends RichSinkFunction<WaterSensor> {
        private Connection conn;
        private PreparedStatement ps;

        //建立到mysql的连接
        @Override
        public void open(Configuration parameters) throws Exception {

            String url = "jdbc:mysql://hadoop102:3306/test?useSSL=false";
            conn = DriverManager.getConnection(url, "root", "123456");

        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            ps = conn.prepareStatement("insert into sensor values(?, ?, ?)");
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
            ps.execute();

        }

        //关闭资源
        @Override
        public void close() throws Exception {
            if (conn != null) {
                conn.close();
            }
        }
    }
}