package cn.avengers.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * Author ZengZihang
 * Desc
 * 1.ds.print 直接输出控制台
 * 2.ds.printToErr() 直接输出到控制台，用红色
 * 3.ds.collect() 将分布式数据收集为本地集合
 * 4.ds.setParallelism(1).writeAsText("本地/HDFS的path","WriteMode.OverWRITE")
 */
public class SinkDemo01 {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStream<String> ds = env.readTextFile("data/input/words.txt");

        //3.transformation
        //4.sink
        ds.print();
        ds.printToErr();
        ds.writeAsText("hdfs://node3/words.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(2);

        //注意：
        //Parallelism=1为文件
        //Parallelism>1为文件夹

        //5.execute
        env.execute();

    }

}
