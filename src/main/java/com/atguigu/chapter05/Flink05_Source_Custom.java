package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @author CoderHyh
 * @create 2022-03-18 18:07
 */
public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new MySource("hadoop102", 9999))
                .print();

        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private String host;
        private int port;
        private volatile boolean isRunning = true;
        private Socket socket;

        public MySource(String host, int port) {
            this.host = host;
            this.port = port;
        }

        //run()方法：使用运行时上下文对象（SourceContext）向下游发送数据
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            // 实现一个从socket读取数据的source
            socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line = null;
            while (isRunning && (line = reader.readLine()) != null) {
                String[] split = line.split(",");
                ctx.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
            }
        }

        /**
         * 大多数的source在run方法内部都会有一个while循环,
         * 当调用这个方法的时候, 应该可以让run方法中的while循环结束
         */

        //cancel()方法：通过标识位控制退出循环，来达到中断数据源的效果
        @Override
        public void cancel() {
            isRunning = false;
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}