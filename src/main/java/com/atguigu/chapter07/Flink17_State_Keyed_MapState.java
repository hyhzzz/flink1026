package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-19 13:29
 */
public class Flink17_State_Keyed_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(2);
        env
                .socketTextStream("hadoop102", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    //定义状态
                    private MapState<Integer, String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        mapState = this
                                .getRuntimeContext()
                                .getMapState(
                                        new MapStateDescriptor<Integer, String>("mapState", Integer.class, String.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        //使用状态
                        if (!mapState.contains(value.getVc())) {
                            out.collect(value);
                            mapState.put(value.getVc(), "随意");
                        }
                    }
                });
        env.execute();
    }
}
