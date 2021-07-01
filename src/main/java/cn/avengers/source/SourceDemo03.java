package cn.avengers.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author ZengZihang
 * Desc
 * SocketSource nc -lk 9999
 */
public class SourceDemo03 {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStream<String> line = env.socketTextStream("node1", 9999);

        DataStream<String> line_filter = line.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String input) throws Exception {
                return (input.equals("avengers assemble")  || input.equals("hello flink hello spark hello hadoop"));
            }
        });

        //3.Transformation
        DataStream<Tuple2<String, Integer>> result = line_filter.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }

            }
        }).keyBy(t -> t.f0).sum(1);

        //4.sink
        result.print();

        //5.execute
        env.execute();

    }

}
