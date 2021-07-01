package cn.avengers.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

/**
 * Author ZengZihang
 * Desc
 * connect and union
 *
 */
public class TransformationDemo02 {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStream<String> ds1 = env.fromElements("1", "2", "3");
        DataStream<String> ds2 = env.fromElements("hadoop", "spark", "flink");
        DataStream<Long> ds3 = env.fromElements(1L, 2L, 3L);

        //3.transformation
        DataStream<String> result1 = ds1.union(ds2);
        ConnectedStreams<String, Long> connect = ds1.connect(ds3);

        DataStream<Tuple2<String, Integer>> result2 = connect.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return value.toString();
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        }).keyBy(t -> t.f0).sum(1);


        //4.sink
        result1.print();
        result2.print();



        //5.execute
        env.execute();

    }
}
