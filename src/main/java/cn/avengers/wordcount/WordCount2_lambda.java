package cn.avengers.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Author ZengZihang
 * Desc
 * 需求：使用Flink完成WordCount-DataStream--使用Lambda表达式
 * 编码步骤
 * 1.准备环境-env
 * 2.准备数据-source
 * 3.处理数据-transformation
 * 4.输出结果-sink
 * 5.触发执行-execute
 *
 */
public class WordCount2_lambda {

    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //2.准备数据-source
        DataStreamSource<String> line = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");

        //3.处理数据-transformation
        //3.1每一行数据按照空格切分成一个个的单词组成一个集合
        /*
        public interface FlatMapFunction<T, O> extends Function, Serializable {
            void flatMap(T value, Collector<O> out) throws Exception;
        }
         */
        //lamda表达式的语法：
        //(参数) -> (方法体/函数体)
        //lamda表达式就是一个函数，函数本质就是对象
        //关于Java8的Stream的详细用法自行百度
        DataStream<String> words = line.flatMap(
                (String value, Collector<String> out) -> Arrays.stream(value.split(" ")).forEach(out::collect)
        ).returns(Types.STRING);

        //3.2对集合中的每个单词记为1
        /*
        public interface MapFunction<T, O> extends Function, Serializable {
            O map(T value) throws Exception;
        }
         */
        //接口、抽象类不能实例化与匿名内部类是不矛盾的，因为匿名内部类的实质是创建了一个没有名字的子类。相当于创建了一个新的类，并不是实例化接口和抽象类
        DataStream<Tuple2<String, Integer>> tuple = words.map(
                (String word) -> Tuple2.of(word, 1),
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                })
        );

        //3.3对数据按照单词(key)进行分组
        //0表示按照tuple中的索引为0的字段，也就是key单词进行分组
        KeyedStream<Tuple2<String, Integer>, String> group = tuple.keyBy(t -> t.f0);

        //3.4对各个组内的数据按照数量来进行聚合就是sum
        //1表示tuple中的索引为1的字段也就是按照数量进行聚合累加
        DataStream<Tuple2<String, Integer>> result = group.sum(1);

        //4.输出结果-sink
        result.print();

        //5.触发执行-execute
        env.execute();

    }
}
