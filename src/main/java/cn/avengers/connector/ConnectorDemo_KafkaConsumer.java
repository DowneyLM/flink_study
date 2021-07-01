package cn.avengers.connector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Author ZengZihang
 *
 * 需求:使用flink-connector-kafka_2.12中的FlinkKakfaConsumer消费kafka中的数据做WordCount
 * 需要设置如下参数：
 * 1.订阅的主题
 * 2.反序列化规则
 * 3.消费者属性-集群地址
 * 4.消费者属性-消费组id(如果不设置，会有默认的，但是默认的不方便管理)
 * 5.消费者属性-offset重置规则，如earlisest/lastest..
 * 6.动态分区检测（当kafka的分区数变化/增加时，Flink能够检测到）
 * 7.如果没有设置Checkpoint，那么可以设置自动提交offset，后续学习了Checkpoint会把offset随着做Checkpoint的时候提交到Checkpoint和默认主题中
 */
public class ConnectorDemo_KafkaConsumer {

    public static void main(String[] args) throws Exception {

        //1。env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.Source
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        props.setProperty("group.id", "flink");
        props.setProperty("auto.offset.reset","latest");
        props.setProperty("flink.partition-discovery.interval-millis","5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.commit.interval.ms","2000");

        //KafkaSource就是KafaConsumer
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink_kafka", new SimpleStringSchema(), props);

        kafkaSource.setStartFromGroupOffsets();//设置从记录的offset开始消费，如果没有记录从auto.offset.reset配置开始消费
        //kafkaSource.setStartFromEarliest();//设置直接从Earliest消费，和auto.offset.reset配置无关
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.Transformation
        DataStream<Tuple2<String, Integer>> result = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0).sum(1);

        //4.sink
        result.print();

        //5.execute
        env.execute();

    }

}
