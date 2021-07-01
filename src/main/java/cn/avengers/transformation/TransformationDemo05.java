package cn.avengers.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Author ZengZihang
 * Desc
 *
 */
public class TransformationDemo05 {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStream<Long> ds = env.fromSequence(1, 10000);

        //3.transformation
        DataStream<Long> filter = ds.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long num) throws Exception {
                return num > 10;
            }
        });

        //接下来使用map操作，将数据转为（分区编号/子任务编号，数据）
        //Rich表示多功能的，比MapFuction要多一些API可以供我们使用
        DataStream<Tuple2<Integer, Integer>> result_1 = filter.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {

            @Override
            public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                //获取分区编号/子任务编号
                int id = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(id, 1);
            }
        }).keyBy(t -> t.f0).sum(1);

        DataStream<Tuple2<Integer, Integer>> result_2 = filter.rebalance().map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                //获取分区编号/子任务编号
                int id_2 = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(id_2, 1);
            }
        }).keyBy(t -> t.f0).sum(1);


        //4.sink
        result_2.print();

        //5.execute
        env.execute();

    }
}
