package cn.avengers.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Author ZengZihang
 * Desc
 * 1.env.readTextFile(本地/HDFS/文件夹)；//压缩文件也可以
 */
public class SourceDemo02 {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        //* 1 env.readTextFile(本地/HDFS/文件夹)；//压缩文件也可以
        DataStream<String> ds1 = env.readTextFile("data/input/words.txt");
        DataStream<String> ds2 = env.readTextFile("data/input/dir");
        DataStream<String> ds3 = env.readTextFile("hdfs://node3:8020//1615307271898");
        DataStream<String> ds4 = env.readTextFile("data/input/hello.zip");

        //3.Transformation
        //4.sink
        ds1.print();
        ds2.print();
        ds3.print();
        ds4.print();

        //5.execute
        env.execute();

    }

}
