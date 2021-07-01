package cn.avengers.connector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;


/**
 * Author ZengZihang
 *
 * Desc
 * 需求:
 * 接收消息并做WordCount
 * 最后将结果保存到Redis
 * 注意:存储到Redis的数据结构，使用hash也就是map
 * key            value
 * WordCount    (单词，数量）
 */
public class ConnectorDemo_Redis {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStream<String> line = env.socketTextStream("node1", 9999);

        //3.transformation
        DataStream<Tuple2<String, Integer>> result = line.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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

        //最后结果保存到Redis
        //1.创建RedisSink之前需要创建RedisConfig
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("node1").build();

        //连接集群版Redis
        //HashSet<InetSocketAddress> nodes = new HashSet<>(Arrays.asList(new InetSocketAddress(InetAddress.getByName("node1"), 6379),new InetSocketAddress(InetAddress.getByName("node2"), 6379),new InetSocketAddress(InetAddress.getByName("node3"), 6379)));
        //FlinkJedisClusterConfig conf2 = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build();
        //连接哨兵版Redis
        //Set<String> sentinels = new HashSet<>(Arrays.asList("node1:26379", "node2:26379", "node3:26379"));
        //FlinkJedisSentinelConfig conf3 = new FlinkJedisSentinelConfig.Builder().setMasterName("mymaster").setSentinels(sentinels).build();

        //创建并且使用RedisSink
        result.addSink(new RedisSink<Tuple2<String, Integer>>(conf,new RedisWordCountMapper()));

        //5.execute
        env.execute();
    }

    /**
     * 定义一个Mapper用来指定存储到Redis中的数据结构
     */
    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String,Integer>>{


        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"wordcount");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }




}
