package com.atguigu.chapter07;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-19 12:32
 */
public class Flink14_State_Operator_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(2);
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop102", 8888);

        //定义状态并广播
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);

        // 广播流
        BroadcastStream<String> broadcastStream = controlStream.broadcast(stateDescriptor);
        dataStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, String>() {

                    //处理数据业务逻辑
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                        // 从广播状态中取值, 不同的值做不同的业务
                        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                        if ("1".equals(state.get("switch"))) {
                            out.collect("切换到1号配置....");
                        } else if ("0".equals(state.get("switch"))) {
                            out.collect("切换到0号配置....");
                        } else {
                            out.collect("切换到其他配置....");
                        }
                    }

                    //处理广播数据，把收到的值，放入广播状态，处理逻辑中就可以拿到这个广播状态
                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                        // 把值放入广播状态
                        state.put("switch", value);
                    }
                })
                .print();

        env.execute();
    }
}
