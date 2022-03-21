package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author CoderHyh
 * @create 2022-03-19 13:26
 */
public class Flink16_State_Keyed_ListState {
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
                .process(new KeyedProcessFunction<String, WaterSensor, List<Integer>>() {
                    //定义状态
                    private ListState<Integer> vcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        vcState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcState", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<List<Integer>> out) throws Exception {
                        vcState.add(value.getVc());
                        //1. 获取状态中所有水位高度, 并排序
                        List<Integer> vcs = new ArrayList<>();
                        for (Integer vc : vcState.get()) {
                            vcs.add(vc);
                        }
                        // 2. 降序排列
                        vcs.sort((o1, o2) -> o2 - o1);
                        // 3. 当长度超过3的时候移除最后一个
                        if (vcs.size() > 3) {
                            vcs.remove(3);
                        }
                        vcState.update(vcs);
                        out.collect(vcs);
                    }
                })
                .print();
        env.execute();
    }
}
