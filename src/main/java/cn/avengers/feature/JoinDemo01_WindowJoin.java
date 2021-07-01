package cn.avengers.feature;


import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Author ZengZihang
 * Desc 演示Flink双流Join-WindowJoin
 */
public class JoinDemo01_WindowJoin {

    public static void main(String[] args) throws Exception {

        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.sink
        //商品数据流
        DataStreamSource<Goods> goodsDS = env.addSource(new GoodsSource());
        //订单数据流
        DataStreamSource<OrderItem> orderItemDS = env.addSource(new OrderItemSource());

        //给数据添加水印（这里简单一点直接使用系统时间作为事件时间）
        /*
        SingleOutputStreamOperator<OrderItem> orderItemDSWithWatermark = orderItemDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderItem>forBoundedOutOfOrderness(Duration.ofSeconds(3))//指定maxOoutOfOrderness最大无序度/最大允许的延迟时间/乱序时间
                        .withTimestampAssigner((orderItem, timestamp) -> orderItem.getEventTime)
        );
         */
        SingleOutputStreamOperator<Goods> goodsDSWithWatermark = goodsDS.assignTimestampsAndWatermarks(new GoodsWatermark());
        SingleOutputStreamOperator<OrderItem> orderItemWithWatermark = orderItemDS.assignTimestampsAndWatermarks(new OrderItemWatermark());

        //TODO 2.transformation -- 这里是重点
        //商品类(商品id,商品名称,商品价格)
        //订单明细类(订单id,商品id,商品数量)
        //关联结果(商品id,商品名称,商品数量,商品价格*商品数量)
        DataStream<FactOrderItem> resultDS = goodsDSWithWatermark.join(orderItemWithWatermark)
                .where(Goods::getGoodsId)
                .equalTo(OrderItem::getGoodsId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Goods, OrderItem, FactOrderItem>() {
                    @Override
                    public FactOrderItem join(Goods goods, OrderItem orderItem) throws Exception {
                        FactOrderItem result = new FactOrderItem();
                        result.setGoodsId(goods.getGoodsId());
                        result.setGoodsName(goods.getGoodsName());
                        result.setCount(new BigDecimal(orderItem.getCount()));
                        result.setTotalMoney(new BigDecimal(orderItem.getCount()).multiply(goods.getGoodsPrice()));
                        return result;
                    }
                });

        //TODO 3.sink
        resultDS.print();

        //TODO 4.execute
        env.execute();

    }


    //商品类(商品id，商品名称，商品价格)
    @Data
    public static class Goods{

        private String goodsId;
        private String goodsName;
        private BigDecimal goodsPrice;
        private static List<Goods> GOODS_LIST;
        private static Random r;

        static {
            r = new Random();
            GOODS_LIST = new ArrayList<>();
            GOODS_LIST.add(new Goods("1", "小米12", new BigDecimal(4890)));
            GOODS_LIST.add(new Goods("2", "iphone12", new BigDecimal(12000)));
            GOODS_LIST.add(new Goods("3", "MacBookPro", new BigDecimal(15000)));
            GOODS_LIST.add(new Goods("4", "Thinkpad X1", new BigDecimal(9800)));
            GOODS_LIST.add(new Goods("5", "MeiZu One", new BigDecimal(3200)));
            GOODS_LIST.add(new Goods("6", "Mate 40", new BigDecimal(6500)));
        }

        public static Goods randomGoods(){
            int rIndex = r.nextInt(GOODS_LIST.size());
            return GOODS_LIST.get(rIndex);
        }

        public Goods(){

        }

        public Goods(String goodsId,String goodsName,BigDecimal goodsPrice){
            this.goodsId = goodsId;
            this.goodsName = goodsName;
            this.goodsPrice = goodsPrice;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }

    //订单明细表（订单id，商品id，商品数量）
    @Data
    public static class OrderItem{

        private String itemId;
        private String goodsId;
        private Integer count;

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }

    //关联结果表（商品id，商品名称，商品数量，商品价格*商品数量）
    @Data
    public static class FactOrderItem{

        private String goodsId;
        private String goodsName;
        private BigDecimal count;
        private BigDecimal totalMoney;

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }

    //实时生成商品数据流
    //构建一个商品Stream源（这个好比就是维表）
    public static class GoodsSource extends RichSourceFunction<Goods>{

        private Boolean isCancel;

        @Override
        public void open(Configuration parameters) throws Exception {
            isCancel = false;
        }

        @Override
        public void run(SourceContext<Goods> sourceContext) throws Exception {
            while (!isCancel){
                Goods.GOODS_LIST.stream().forEach(goods -> sourceContext.collect(goods));
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isCancel = true;
        }
    }

    //实时生成订单数据流
    //构建订单明细Stream流
    public static class OrderItemSource extends RichSourceFunction<OrderItem>{

        private Boolean isCancel;
        private Random r;

        @Override
        public void open(Configuration parameters) throws Exception {
            isCancel = false;
            r = new Random();
        }

        @Override
        public void run(SourceContext<OrderItem> sourceContext) throws Exception {

            while (!isCancel){
                Goods goods = Goods.randomGoods();
                OrderItem orderItem = new OrderItem();
                orderItem.setGoodsId(goods.getGoodsId());
                orderItem.setCount(r.nextInt(10) + 1);
                orderItem.setItemId(UUID.randomUUID().toString());
                sourceContext.collect(orderItem);
                orderItem.setGoodsId("111");
                TimeUnit.SECONDS.sleep(1);
            }

        }

        @Override
        public void cancel() {
            isCancel = true;
        }
    }

    //构建水印分配器，学习测试直接使用系统时间了
    public static class GoodsWatermark implements WatermarkStrategy<Goods>{

        @Override
        public TimestampAssigner<Goods> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element,recordTimestamp) -> System.currentTimeMillis();
        }

        @Override
        public WatermarkGenerator<Goods> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<Goods>() {
                @Override
                public void onEvent(Goods goods, long l, WatermarkOutput watermarkOutput) {
                    watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis()));
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                    watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
            };
        }
    }


    //构建水印分配器，学习测试直接使用系统时间
    public static class OrderItemWatermark implements WatermarkStrategy<OrderItem>{

        @Override
        public TimestampAssigner<OrderItem> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element,recordTimestamp) -> System.currentTimeMillis();
        }

        @Override
        public WatermarkGenerator<OrderItem> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<OrderItem>() {
                @Override
                public void onEvent(OrderItem orderItem, long l, WatermarkOutput watermarkOutput) {
                    watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis()));
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                    watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
            };
        }
    }

}
