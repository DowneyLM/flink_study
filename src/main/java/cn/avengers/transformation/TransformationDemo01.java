package cn.avengers.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author ZengZihang
 * Desc
 *
 */
public class TransformationDemo01 {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStream<String> line = env.socketTextStream("node1", 9999);

        //3.transformation
        DataStream<Tuple2<String, Integer>> result = line.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String word) throws Exception {
                return !word.contains("fuck");
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        }).keyBy(word -> word.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0,t1.f1 + t2.f1);
            }
        });


        //4.sink
        result.print();


        //5.execute
        env.execute();

    }
}
