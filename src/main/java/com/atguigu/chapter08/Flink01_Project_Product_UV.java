package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author CoderHyh
 * @create 2022-03-19 17:04
 */
public class Flink01_Project_Product_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
                })
                .assignTimestampsAndWatermarks(wms)
                .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {
                    private MapState<Long, Object> userIdState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userIdState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Object>("userIdState", Long.class, Object.class));
                    }

                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<UserBehavior, String, String, TimeWindow>
                                                .Context context, Iterable<UserBehavior> iterable, Collector<String>
                                                collector) throws Exception {


                        for (UserBehavior ub : iterable) {
                            userIdState.put(ub.getUserId(), new Object());
                        }
                        Long sum = 0L;
                        for (Long uid : userIdState.keys()) {
                            sum++;
                        }
                        String msg = "w_start=" + context.window().getStart() + ",w_end=" + context.window().getEnd() + "duv是:" + sum;
                        collector.collect(msg);
                    }
                }).print();

        env.execute();
    }
}
