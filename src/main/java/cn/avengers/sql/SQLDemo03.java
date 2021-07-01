package cn.avengers.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Author ZengZihang
 *
 * Desc 演示Flink Table&SQL 案例 - 将DataStream数据转Table和View然后使用SQL进行统计查询
 */
public class SQLDemo03 {

    public static void main(String[] args) throws Exception {

        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        //TODO 1.source
        DataStreamSource<WC> wordDS = env.fromElements(
                new WC("hello", 1),
                new WC("world", 1),
                new WC("hello", 1)
        );


        //TODO 2.transformation
        //将DataStream数据转Table和View，然后查询
        Table table = tenv.fromDataStream(wordDS);

        //使用Table风格查询 /DSL
        Table resultTable = table.groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .filter($("frequency").isEqual(2));

        DataStream<Tuple2<Boolean, WC>> resultDS = tenv.toRetractStream(resultTable, WC.class);

        //TODO 3.sink
        resultDS.print();

        //TODO 4.execute
        env.execute();

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        public String word;
        public long frequency;
    }
}
