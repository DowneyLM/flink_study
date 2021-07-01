package cn.avengers.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Author ZengZihang
 * Desc
 * 需求：使用Flink完成WordCount-DataStream--使用Lambda表达式--修改代码使适合仔Yarn运行
 * 编码步骤
 * 1.准备环境-env
 * 2.准备数据-source
 * 3.处理数据-transformation
 * 4.输出结果-sink
 * 5.触发执行-execute 批处理不需要调用流处理需要
 */


public class WordCount3_Yarn {

    public static void main(String[] args) throws Exception {
        //获取参数
        ParameterTool params = ParameterTool.fromArgs(args);
        String output = null;
        if(params.has("output")){
            output = params.get("output");
        }else{
            output = "hdfs://node1:8020/" + System.currentTimeMillis();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> line = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");

        DataStream<Tuple2<String, Integer>> result = line.flatMap(
                (String value, Collector<String> out) -> Arrays.stream(value.split(" ")).forEach(out::collect)
        ).returns(Types.STRING)
                .map((String word) -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0).sum(1);

        result.print();

        //如果执行报hdfs权限相关错误,可以执行 hadoop fs -chmod -R 777  /
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        result.writeAsText(output).setParallelism(1);

        //5.触发执行-execute
        env.execute();

    }

}
