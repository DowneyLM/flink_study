package cn.avengers.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Author ZengZihang
 * Desc
 * 拆分
 *
 */
public class TransformationDemo03 {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStreamSource<Long> ds = env.fromSequence(1, 100);

        //3.transformation
        
        //定义2个输出的标签
        OutputTag<Long> tag_even = new OutputTag<Long>("偶数", TypeInformation.of(Long.class));
        OutputTag<Long> tag_odd = new OutputTag<Long>("奇数"){};
        
        //对ds中的数据进行处理
        SingleOutputStreamOperator<Long> process = ds.process(new ProcessFunction<Long, Long>() {

            @Override
            public void processElement(Long value, Context context, Collector<Long> collector) throws Exception {

                if (value % 2 == 0) {
                    context.output(tag_even, value);
                } else {
                    context.output(tag_odd, value);
                }
            }
        });

        //取出标好的数据
        DataStream<Long> evenResult = process.getSideOutput(tag_even);
        DataStream<Long> oddResult = process.getSideOutput(tag_odd);

        //4.sink
        evenResult.print("偶数");
        oddResult.print("奇数");


        //5.execute
        env.execute();

    }
}
