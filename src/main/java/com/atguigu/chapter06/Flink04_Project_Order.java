package com.atguigu.chapter06;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author CoderHyh
 * @create 2022-03-19 1:05
 */
public class Flink04_Project_Order {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<OrderEvent> orderEventDS = env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3]));

                });
        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new TxEvent(datas[0], datas[1], Long.valueOf(datas[2]));
                });

        ConnectedStreams<OrderEvent, TxEvent> orderAndTx = orderEventDS.connect(txDS);

        orderAndTx
                .keyBy("txId", "txId")
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    Map<String, OrderEvent> orderMap = new HashMap<>();
                    Map<String, TxEvent> txMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (txMap.containsKey(value.getTxId())) {
                            out.collect("订单: " + value + " 对账成功");
                            txMap.remove(value.getTxId());
                        } else {
                            orderMap.put(value.getTxId(), value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (orderMap.containsKey(value.getTxId())) {
                            OrderEvent orderEvent = orderMap.get(value.getTxId());
                            out.collect("订单: " + orderEvent + " 对账成功");
                            orderMap.remove(value.getTxId());
                        } else {
                            txMap.put(value.getTxId(), value);
                        }
                    }
                })
                .print();
        env.execute();

    }
}
