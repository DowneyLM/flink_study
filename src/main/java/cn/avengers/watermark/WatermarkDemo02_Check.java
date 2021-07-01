package cn.avengers.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

/**
 * Author ZengZihang
 * Desc 演示基于事件时间的窗口计算+watermark解决一定程度上的数据乱序/延迟到达的问题
 */

public class WatermarkDemo02_Check {

    public static void main(String[] args) throws Exception {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.source
        DataStreamSource<Order> orderDS = env.addSource(new SourceFunction<Order>() {

            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> sourceContext) throws Exception {

                Random random = new Random();

                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(3);
                    int money = random.nextInt(100);
                    //模拟数据延迟和乱序
                    long eventTime = System.currentTimeMillis() - random.nextInt(7) * 1000;
                    System.out.println("发送的数据为：" + userId + ":" + dateFormat.format(eventTime));
                    sourceContext.collect(new Order(orderId, userId, money, eventTime));
                    //TimeUnit.SECONDS.sleep(1);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });


        //TODO 3.transformation
        //SingleOutputStreamOperator<Order> waterMark = orderDS.assignTimestampsAndWatermarks(
        //        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
        //                .withTimestampAssigner((event, timestamp) -> event.getEventTime())
        //);

        SingleOutputStreamOperator<Order> orderDsCheck = orderDS.assignTimestampsAndWatermarks(
                new WatermarkStrategy<Order>() {
                    @Override
                    public WatermarkGenerator<Order> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Order>() {

                            private int userId = 0;
                            private long eventTime = 0L;
                            private final long outOfOrdernessMillis = 3000;
                            private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;


                            @Override
                            public void onEvent(Order order, long eventTimestamp, WatermarkOutput watermarkOutput) {
                                userId = order.userId;
                                eventTime = order.eventTime;
                                //System.out.println(eventTime == eventTimestamp);
                                maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                                //Watermark = 当前最大事件时间- 最大允许的延迟时间或乱序时间
                                Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
                                System.out.println("key:" + userId
                                        + ",系统时间：" + dateFormat.format(System.currentTimeMillis())
                                        + ",事件时间：" + dateFormat.format(eventTime)
                                        + ",水印时间：" + dateFormat.format(watermark.getTimestamp())
                                );
                                watermarkOutput.emitWatermark(watermark);
                            }
                        };
                    }
                }.withTimestampAssigner((order, timestamp) -> order.getEventTime())
        );


        SingleOutputStreamOperator<String> result = orderDsCheck.keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //把apply中的函数应用在窗口中的数据上
                //WindowFunction<IN, OUT, KEY, W extends Window>
                .apply(new WindowFunction<Order, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key, TimeWindow window, Iterable<Order> orders, Collector<String> collector) throws Exception {

                        //用来存放当前窗口的数据的格式化后的事件时间
                        ArrayList<String> list = new ArrayList<>();

                        for (Order order : orders) {
                            Long eventTime = order.eventTime;
                            String format = dateFormat.format(eventTime);
                            list.add(format);
                        }

                        String start = dateFormat.format(window.getStart());
                        String end = dateFormat.format(window.getEnd());
                        //现在就已经获取到了当前窗口的开始和结束时间,以及属于该窗口的所有数据的事件时间,把这些拼接并返回
                        String outStr = String.format("key:%s,窗口开始结束:[%s~%s),属于该窗口的事件时间:%s", key.toString(), start, end, list.toString());
                        collector.collect(outStr);
                    }
                });


        //TODO 4.sink
        result.print();

        //TODO 5.execute
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
