package cn.avengers.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
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
public class TransformationDemo06 {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStream<String> ds = env.readTextFile("data/input/words.txt");

        //3.transformation
        DataStream<Tuple2<String, Integer>> tuple = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        DataStream<Tuple2<String, Integer>> result_1 = tuple.global();
        DataStream<Tuple2<String, Integer>> result_2 = tuple.broadcast();
        DataStream<Tuple2<String, Integer>> result_3 = tuple.forward();
        DataStream<Tuple2<String, Integer>> result_4 = tuple.shuffle();
        DataStream<Tuple2<String, Integer>> result_5 = tuple.rebalance();
        DataStream<Tuple2<String, Integer>> result_6 = tuple.rescale();
        DataStream<Tuple2<String, Integer>> result_7 = tuple.partitionCustom(new Partitioner<String>() {

            @Override
            public int partition(String key, int i) {
                return key.equals("hello") ? 0 : 1;
            }

        }, t -> t.f0);


        //4.sink
        //result_1.print();
        //result_2.print();
        //result_3.print();
        //result_4.print();
        //result_5.print();
        //result_6.print();
        result_7.print();


        //5.execute
        env.execute();

    }
}
