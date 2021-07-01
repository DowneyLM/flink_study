package cn.avengers.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.linq4j.Ord;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.w3c.dom.TypeInfo;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * Author ZengZihang
 * Desc 演示基于事件时间的窗口计算+watermark解决一定程度上的数据乱序/延迟到达的问题
 * 并且使用outputTag + allowedLateness来解决数据丢失问题(解决迟到/延迟严重的数据的丢失问题)
 */

public class WatermarkDemo03 {

    public static void main(String[] args) throws Exception {

        FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss");

        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        
        //TODO 2.source
        DataStream<Order> orderDS = env.addSource(new SourceFunction<Order>() {

            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(101);
                    //随机模拟延迟
                    long eventTime = System.currentTimeMillis() - random.nextInt(20) * 1000;
                    ctx.collect(new Order(orderId, userId, money, eventTime));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        //注意:下面的代码使用的是Flink1.12中新的API
        //每隔5s计算最近5s的数据求每个用户的订单总金额,要求:基于事件时间进行窗口计算+Watermaker
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//在新版本中默认就是EventTime
        //设置Watermarker = 当前最大的事件时间 - 最大允许的延迟时间或乱序时间
        SingleOutputStreamOperator<Order> orderDsWithWatermark = orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))//指定maxOutOfOrderness最大无序度/最大允许的延迟时间/乱序时间
                        .withTimestampAssigner((order, timestamp) -> order.getEventTime()) //指定事件时间序列
        );


        OutputTag<Order> seriousLate = new OutputTag<>("seriousLate", TypeInformation.of(Order.class));

        SingleOutputStreamOperator<Order> result1 = orderDsWithWatermark.keyBy(Order::getUserId).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(seriousLate)
                .sum("money");

        DataStream<Order> result2 = result1.getSideOutput(seriousLate);

        //TODO 3.sink

        result1.print("正常到达或者稍微延迟的数据：");
        result2.print("迟到严重的数据并丢弃后单独收集的数据：");

        //TODO 4.execute
        env.execute();


    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }

}
