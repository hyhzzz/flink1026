package com.atguigu.chapter08;

import com.atguigu.bean.LoginEvent;
import com.atguigu.util.MyUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @author CoderHyh
 * @create 2022-03-21 17:19
 */
public class Flink04_Project_Login {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建WatermarkStrategy
        WatermarkStrategy<LoginEvent> wms = WatermarkStrategy
                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                });
        env
                .readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new LoginEvent(Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000L);
                })
                .assignTimestampsAndWatermarks(wms)
                // 按照用户id分组
                .keyBy(LoginEvent::getUserId)
                .process(new KeyedProcessFunction<Long, LoginEvent, String>() {
                    private ListState<Long> failTsState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        failTsState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("failTsState", Long.class));
                    }

                    @Override
                    public void processElement(LoginEvent event,
                                               KeyedProcessFunction<Long, LoginEvent, String>.Context ctx, Collector<String> out) throws Exception {

                     /*
                        统计连续失败的次数:
                            1. 把失败的时间戳放入到List中,
                            2. 当List中的长度到达2的时候, 判断这个两个时间戳的差是否小于等于2s
                            3. 如果是, 则这个用户在恶意登录
                            4. 否则不是, 然后删除List的第一个元素用于保持List的长度为2
                            6. 如果出现登录成功, 则需要清空List集合, 重新开始计算
                     */

                        if ("fail".equals(event.getEventType())) {
                            failTsState.add(event.getEventTime());

                            List<Long> list = MyUtil.toList(failTsState.get());
                            if (list.size() == 2) {//如果有两个失败，则按照时间戳进行升序排序
                                list.sort(Long::compareTo);

                                if ((list.get(1) - list.get(0)) >= 2000) {
                                    out.collect(event.getUserId() + "正在恶意登录");
                                } else {
                                    failTsState.clear();
                                    failTsState.add(list.get(1));
                                }
                            }
                        } else if ("success".equals(event.getEventType())) {
                            failTsState.clear();
                        }
                    }
                }).print();
        env.execute();

    }
}
