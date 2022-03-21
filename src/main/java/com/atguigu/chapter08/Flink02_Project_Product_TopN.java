package com.atguigu.chapter08;


import com.atguigu.bean.HotItem;
import com.atguigu.bean.UserBehavior;
import com.atguigu.util.MyUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @author CoderHyh
 * @create 2022-03-19 17:22
 */
public class Flink02_Project_Product_TopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3],
                            Long.valueOf(split[4]));
                })
                .filter(data -> "pv".equals(data.getBehavior())) // 过滤出来点击数据
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(
                                Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.getTimestamp() * 1000L;
                            }
                        })
                )

                .keyBy(UserBehavior::getItemId) // 按照产品id进行分组
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(UserBehavior value, Long acc) {
                                   return acc + 1;
                               }

                               @Override
                               public Long getResult(Long acc) {
                                   return acc;
                               }

                               @Override
                               public Long merge(Long a, Long b) {
                                   return null;
                               }
                           },
                        new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                            @Override
                            public void process(Long key,
                                                ProcessWindowFunction<Long, HotItem, Long, TimeWindow>.Context context,
                                                Iterable<Long> elements,
                                                Collector<HotItem> out) throws Exception {

                                Long count = elements.iterator().next();
                                Long wEnd = context.window().getEnd();
                                out.collect(new HotItem(key, count, wEnd));

                            }
                        }
                )
                .keyBy(HotItem::getWindowEndTime)
                .process(new KeyedProcessFunction<Long, HotItem, String>() {
                    private ListState<HotItem> hotItemState;
                    private ValueState<Long> wEndState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hotItemState = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("hotItemState", HotItem.class));
                        wEndState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("wEndState", Long.class));
                    }

                    @Override
                    public void processElement(HotItem hotItem,
                                               KeyedProcessFunction<Long, HotItem, String>.Context ctx,
                                               Collector<String> out) throws Exception {


                        hotItemState.add(hotItem);
                        if (wEndState.value() == null) {
                            //定时器时间多少
                            long timerTs = hotItem.getWindowEndTime() + 10000;
                            ctx.timerService().registerEventTimeTimer(timerTs);

                            wEndState.update(timerTs);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, HotItem, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

                        List<HotItem> list = MyUtil.toList(hotItemState.get());
                        //排序
                        list.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));

                        //取前三

                        StringBuilder sb = new StringBuilder();
                        sb.append("窗口结束时间: " + (timestamp - 1) + "\n");
                        sb.append("---------------------------------\n");
                        for (int i = 0; i < 3; i++) {
                            sb.append(list.get(i) + "\n");
                        }
                        sb.append("---------------------------------\n\n");
                        out.collect(sb.toString());
                    }
                }).print();


        env.execute();
    }
}
