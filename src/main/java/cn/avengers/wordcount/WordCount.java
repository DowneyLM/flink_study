package cn.avengers.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author ZengZihang
 * Desc
 * 需求：使用Flink完成WordCount-DataStream
 * 编码步骤:
 * 1.准备环境-env
 * 2.准备数据-source
 * 3.处理数据-transformation
 * 4.输出结果-sink
 * 5.触发执行-execute
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        //新版本的流批式一API，既支持流处理也支持批处理
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.准备数据-source
        DataStream<String> line = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");

        //3.处理数据-transformation
        //3.1 每一行数据按照空格切分成一个个的单词组成一个集合
        //public interface FlatMapFunction<T, O> extends Function, Serializable {
        //    void flatMap(T var1, Collector<O> var2) throws Exception;
        //}
        DataStream<String> words = line.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                //value就是一行数据
                String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }

            }
        });

        //3.2对集合中的每个单词记为1
        //public interface MapFunction<T, O> extends Function, Serializable {
        //    O map(T var1) throws Exception;
        //}
        DataStream<Tuple2<String, Integer>> tuple = words.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                //进来就是一个个的单词
                return Tuple2.of(word, 1);
            }
        });

        //3.3对数据按照单词(key)进行分组
        //0表示按照tuple中的索引为0的字段，也就是key(单词)进行分组
        //public interface KeySelector<IN, KEY> extends Function, Serializable {
        //    KEY getKey(IN var1) throws Exception;
        //}
        KeyedStream<Tuple2<String, Integer>, String> group = tuple.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0;
            }
        });

        //3.4对各个组内的数据按照数量(value)进行聚合就是求sum
        //1表示按照tuple中的索引为1的字段也就是按照数量进行聚合累加
        DataStream<Tuple2<String, Integer>> result = group.sum(1);

        //4.输出结果-sink
        result.print();

        //5.触发执行-execute
        env.execute(); //DataStream需要调用execute

    }

}
