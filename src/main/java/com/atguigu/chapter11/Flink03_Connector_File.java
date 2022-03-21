package com.atguigu.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author CoderHyh
 * @create 2022-03-20 16:27
 */
public class Flink03_Connector_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(2);

        Schema schema = new Schema();
        schema
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("input/sensor.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                //创建一个动态表
                .createTemporaryTable("sensor");

        //得到table对象
        Table sensor = tableEnv.from("sensor");

        //有table对象可以直接打印 仅仅只是用来做测试，不用转流输出了
        //sensor.execute().print();

        Table resultTable = sensor.groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);
        resultStream.filter(r->r.f0).print();

        env.execute();

    }
}
