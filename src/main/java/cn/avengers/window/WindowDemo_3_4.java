package cn.avengers.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author ZengZihang
 * Desc 演示基于数量的滚动和滑动窗口
 */

public class WindowDemo_3_4 {

    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.source
        DataStream<String> lines = env.socketTextStream("node1", 9999);

        //TODO 3.transformation
        DataStream<CartInfo> carDS = lines.map(new MapFunction<String, CartInfo>() {
            @Override
            public CartInfo map(String s) throws Exception {
                String[] car = s.split(",");
                return new CartInfo(car[0], Integer.parseInt(car[1]));
            }
        });

        //注意：需求中要求的是各个路口/红绿灯的结果，所以需要先分组
        KeyedStream<CartInfo, String> keyedCar = carDS.keyBy(CartInfo::getSensorId);

        //*需求1：设置会话超时时间为10s,10s内没有数据到来,则触发上个窗口的计算(前提是上一个窗口得有数据!)
        SingleOutputStreamOperator<CartInfo> result_1 = keyedCar
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum("count");

        //TODO 4.sink
        result_1.print();
        //result_2.print();

        env.execute();


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CartInfo{
        private String sensorId;//信号灯ID
        private Integer count;
    }

}
