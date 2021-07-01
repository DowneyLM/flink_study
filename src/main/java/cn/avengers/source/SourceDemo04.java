package cn.avengers.source;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.UUID;

/**
 * Author ZengZihang
 * Desc
 * 需求
 *  每隔1秒随机生成一条订单信息（订单ID，用户ID，订单金额，时间戳）
 * 要求
 * --随机生成订单ID(UUID)
 * --随机生成用户ID(0-2)
 * --随机生成订单金额(0-100)
 * --时间戳为当前系统时间
 *
 * API
 * 一般用于学习测试，模拟生成一些数据
 * Flink还提供了数据源接口，我们实现该接口就可以实现自定义数据源，不同的接口有不同的功能，分类如下：
 * SourceFunction：非并行数据源（并行度只能=1）
 * RichSourceFunction：多功能非并行数据源(并行度只能=1)
 * ParallelSourceFunction:并行数据源(并行度能够>=1)
 * RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)--后续学习的Kafka数据源使用的就是该接口
 */
public class SourceDemo04 {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStreamSource<Order> orderDs = env.addSource(new MyOrderSource()).setParallelism(2);


        //3.Transformation

        //4.sink
        orderDs.print();

        //5.execute
        env.execute();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order{
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }

    public static class MyOrderSource extends RichParallelSourceFunction<Order>{

        private Boolean flag = true;

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {

            Random random = new Random();
            while (flag){
                Thread.sleep(1000);
                String id = UUID.randomUUID().toString();
                int userId = random.nextInt(3);
                int money = random.nextInt(101);
                long createTime = System.currentTimeMillis();
                ctx.collect(new Order(id,userId,money,createTime));
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

}
