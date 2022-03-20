package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-18 23:39
 */
public class Flink01_Project_PV {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //读取数据
        DataStreamSource<String> stream = env.readTextFile("input/UserBehavior.csv");
        //对数据进行处理
        //封装pojo
        //去重
        //map成(pv,1)
        //keyby
        //sum

        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = stream.map(
                new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new UserBehavior(Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4]));
                    }
                }
        );

        SingleOutputStreamOperator<UserBehavior> filter = userBehaviorStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return "pv".equalsIgnoreCase(userBehavior.getBehavior());
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> pvToOneStream = filter.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                return Tuple2.of(userBehavior.getBehavior(), 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = pvToOneStream.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        //启动执行环境
        env.execute();
    }
}
