package cn.avengers.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author ZengZihang
 * desc 演示Flick累加器，统计处理的数据条数
 */
public class OtherAPI_Accumulator {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.source
        DataStream<String> ds = env.fromElements("aaa", "bbb", "ccc", "ddd");

        //3.Transformation
        SingleOutputStreamOperator<String> result = ds.map(new RichMapFunction<String, String>() {

            //-1.创建累加器
            private IntCounter elementCounter = new IntCounter();
            Integer count = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //-2.注册累加器
                getRuntimeContext().addAccumulator("elementCounter", elementCounter);
            }

            @Override
            public String map(String vaule) throws Exception {
                //-3.使用累加器
                this.elementCounter.add(1);
                count += 1;
                System.out.println("不使用累加器统计的结果为：" + count);
                return vaule;
            }
        }).setParallelism(2);

        //4.sink
        result.print();

        //5.execute
        //-4.获取加强结果
        JobExecutionResult jobResult = env.execute();
        int nums = jobResult.getAccumulatorResult("elementCounter");
        System.out.println("使用累加器统计的结果为" + nums);

    }



}
