package com.atguigu.chapter07;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-19 13:32
 */
public class Flink18_State_Backend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        //多久做一次Checkpoint 三秒做一次
        env.enableCheckpointing(3000);
        //设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new RocksDBStateBackend(""));

        //env.setStateBackend(new HashMapStateBackend());//1.13版本写法
        //env.getCheckpointConfig().setCheckpointStorage("");//1.13版本写法

        env.execute();

    }
}
