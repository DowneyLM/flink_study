package cn.avengers.action;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Author ZengZihang
 * Desc
 * 1.实时计算出当天零点截止当天时间的销售总额，11月11日，00：00：00 - 23：59：59
 * 2.计算出各个分类的销售额top3
 * 3.每秒钟更新一次统计结果
 */
public class DoubleElevenBigScreem {


    public static void main(String[] args) throws Exception {

        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);//方便观察

        //TODO 2.source
        DataStream<Tuple2<String, Double>> orderDS = env.addSource(new MySource());

        //TODO 3.transformation 初步聚合：每隔1s聚合一下截止到当前时间的各个分类的销售总额
        DataStream<CategoryPojo> aggResult = orderDS
                //分组
                .keyBy(t -> t.f0)
                //如果直接使用之前学习的窗口按照下面的写法表示：
                //每隔1天计算一次
                //.window(TumblingProcessingTimeWindows.of(Time.days(1)));
                //表示每隔1s计算最近一天的数据，但是11月11日，00：01：00运行计算的是：11月10日00：01：00 - 11月11日 00：01：00 --不对！
                //.window(SlidingProcessingTimeWindows.of(Time.days(1),Time.seconds(1)));
                //*例如中国使用UTC+8:00,你需要一天大小的时间窗口，
                //*窗口从当地时间的00：00：00开始，你可以使用{@code of（时间.天(1)），时间.hours（-8）}.
                //下面的代码表示从当天的00：00：00开始计算当天的数据，缺一个触发时机/触发间隔
                //3.1定义一个大小为1天的时间窗口，第二个参数表示中国使用的UTC+08：00时区比UTC时间早
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                //3.2自定义触发时机/触发间隔
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                //.sum()简单聚合
                //3.3自定义聚合和结果收集
                //AggregateFunction<T, ACC, V> aggFunction, WindowFunction<V, R, K, W> windowFunction
                .aggregate(new PriceAggregate(), new WindowResult());//aggregate支持复杂的自定义聚合


        //3.4看一下聚合结果
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=洗护, totalPrice=1509.959949616862, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=美妆, totalPrice=1240.7248295110887, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=乐器, totalPrice=1321.1279123447853, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=运动, totalPrice=1434.2884816963904, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=男装, totalPrice=896.7649141451907, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=女装, totalPrice=1281.9324161494606, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=图书, totalPrice=1409.3521727050415, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=游戏, totalPrice=1446.9489750501534, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=家具, totalPrice=1356.599147265771, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=家电, totalPrice=743.5077166604628, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=户外, totalPrice=1230.2078816871003, dateTime=2021-03-19 20:11:26)
        //初步聚合的各个分类的销售总额> DoubleElevenBigScreem.CategoryPojo(category=办公, totalPrice=1307.8420941020895, dateTime=2021-03-19 20:11:26)
        aggResult.print("初步聚合的各个分类的销售总额");

        //TODO 4.sink-使用上面初步聚合的结果（每隔1s聚合一下截止到当前时间的各个分类的销售总金额），实现业务需求：
        aggResult.keyBy(CategoryPojo::getDateTime)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1))) //每隔1s进行最终的聚合并且输出结果
                //sum 简单聚合
                //.apply()
                .process(new FinalResultWindowProcess()); //ProcessWindowFunction<IN, OUT, KEY, W extends Window> 在ProcessWindowFunction中实现该复杂业务逻辑


        //TODO 5.execute
        env.execute();

    }

    /**
     * 自定义数据源实时产生订单数据Tuple2<分类，金额>
     */
    public static class MySource implements SourceFunction<Tuple2<String,Double>>{

        private boolean flag = true;
        private String[] categroys = {"女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<Tuple2<String, Double>> sourceContext) throws Exception {
            while (flag){
                //随机生成分类和金额
                int index = random.nextInt(categroys.length);
                String category = categroys[index];
                double price = random.nextDouble() * 100 ; //注意nextDouble生成的是[0-1]之间的随机小数，*100之后表示[0~100)的随机小数
                sourceContext.collect(Tuple2.of(category,price));
                Thread.sleep(20);
            }

        }

        @Override
        public void cancel() {
            flag = false;
        }
    }


    /**
     * 自定义聚合函数，指定聚合规则
     * AggregateFunction<IN,ACC,OUT>
     */
    private static class PriceAggregate implements AggregateFunction<Tuple2<String,Double>,Double,Double>{

        //初始化累加器
        @Override
        public Double createAccumulator() {
            return 0D;  //D表示Double，L表示Long
        }

        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return value.f1 + accumulator;
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }



    /**
     * 自定义窗口函数，指定窗口数据收集规则
     * WindowFunction<IN, OUT, KEY, W extends Window>
     */
     public static class WindowResult implements WindowFunction< Double, CategoryPojo, String, TimeWindow >{

         private FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

         //void apply(KEY var1, W var2, Iterable<IN> var3, Collector<OUT> var4) throws Exception;
        @Override
        public void apply(String category, TimeWindow window, Iterable<Double> iterable, Collector<CategoryPojo> collector) throws Exception {
            long currentTimeMillis = System.currentTimeMillis();
            String dateTime = df.format(currentTimeMillis);
            Double totalPrice = iterable.iterator().next();
            collector.collect(new CategoryPojo(category,totalPrice,dateTime));
        }
    }


    /**
     * 用于存储聚合的结果
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CategoryPojo{
        private String category; //分类名称
        private double totalPrice;//该分类总销售额
        private String dateTime;//截止到当前时间，本应该是eventIime，但是我们这里简化了直接用当前系统时间即可
    }

    /**
     * 自定义窗口完成销售总额统计和分类销售额top3统计并且输出
     * extends ProcessWindowFunction<IN, OUT, KEY, W extends Window>
     */
    private static class FinalResultWindowProcess extends ProcessWindowFunction<CategoryPojo,Object,String,TimeWindow> {
        //注意：
        //下面的key/dateTime表示当前这1s的时间
        //elements：表示截止到当前这1s的各个分类的销售数据
        @Override
        public void process(String dateTime, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {
            //1.实时计算出当天零点截止到当前时间的销售总额 11月11日 00：00：00 - 23：59：59
            double total = 0D ; //用来记录销售总额
            //2.计算出各个分类的销售top3：如："女装"：10000 "男装":9000 "图书":8000
            //注意：这里只需要求top3，也就是说只需要排前3名就行，其他的不用管，当然你也可以每次对进来的数据进行排序，但是浪费;
            //所以这里直接使用小顶堆来完成top3排序
            // 70
            //80 90
            //如果进来一个比堆顶元素还有小的，直接不要
            //如果进来一个比堆顶元素还要大的，如85，则直接把堆元素删掉，把85加进去，并且继续按照小顶堆规则排序，小的在上面，大的在下面
            // 80
            //85 90
            //创建一个小顶堆
            Queue<CategoryPojo> queue = new PriorityQueue<>(3//初始容量
                    //正常排序，就是小的在前，大的在后，也就是c1>c2的时候返回1，也就是升序，也就是小顶堆
                    , (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);

            for(CategoryPojo element:elements){
                double price = element.getTotalPrice();
                total += price;
                if (queue.size() < 3){
                    queue.add(element); //或offer入队伍
                }else {
                    if(price >= queue.peek().getTotalPrice()){//peek表示取出堆顶元素但是不删除
                        queue.poll();//移除堆顶元素
                        queue.add(element);
                    }
                }
            }

            //代码走到这里，那么queue存放的就是分类的销售额top3，但是是升序，所以需要改为逆序然后输出
            List<String> top3List = queue.stream().sorted((c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? -1 : 1)
                    .map(c -> "分类：" + c.getCategory() + " 金额：" + c.getTotalPrice())
                    .collect(Collectors.toList());

            //3.每秒钟更新一次统计结果-也就是直接输出
            double roundReuslt = new BigDecimal(total).setScale(2, RoundingMode.HALF_UP).doubleValue(); //四舍五入保留2位小数
            System.out.println("时间：" + dateTime + "总金额：" + roundReuslt);

            System.out.println("top3: \n" + StringUtils.join(top3List,"\n"));
        }
    }




}
