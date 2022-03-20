package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-18 9:16
 * 批处理
 */
public class BathWordCount {
    public static void main(String[] args) throws Exception {
        //获取批的执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件读取数据
        DataSource<String> lineDS = env.readTextFile("input/words.txt");

        //flatmap
        //<String, String>:输入(都是String类型)  输出(String)
        FlatMapOperator<String, String> wordDS = lineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(word);
                }
            }
        });

        //每个单词配置一个1   hello --> (hello,1L)
        MapOperator<String, Tuple2<String, Long>> wordAndOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });

        // group by
        UnsortedGrouping<Tuple2<String, Long>> groupedDS = wordAndOneDS.groupBy(0);

        //sum
        AggregateOperator<Tuple2<String, Long>> result = groupedDS.sum(1);

        result.print();
    }
}
