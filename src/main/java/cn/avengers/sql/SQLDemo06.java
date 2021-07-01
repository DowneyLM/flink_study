package cn.avengers.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Author ZengZihang
 *
 * Desc 演示Flink Table&SQL 案例 - 使用事件时间+WaterMark+window完成订单统计
 */
public class SQLDemo06 {

    public static void main(String[] args) throws Exception {

        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        //TODO 1.source
        TableResult inputTable = tenv.executeSql(
                "CREATE TABLE input_kafka (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `page_id` BIGINT,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'input_kafka',\n" +
                        "  'properties.bootstrap.servers' = 'node1:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'group-offsets',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );

        //TODO 2.transformation
        //编写sql过滤出状态为success的数据
        String sql = "select * from input_kafka where status='success'";
        Table etlResult = tenv.sqlQuery(sql);


        //TODO 3.sink
        DataStream<Tuple2<Boolean, Row>> resultDS = tenv.toRetractStream(etlResult, Row.class);
        resultDS.print();

        TableResult outputTable = tenv.executeSql(
                "CREATE TABLE output_kafka (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `page_id` BIGINT,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'output_kafka',\n" +
                        "  'properties.bootstrap.servers' = 'node1:9092',\n" +
                        "  'format' = 'json',\n" +
                        "  'sink.partitioner' = 'round-robin'\n" +
                        ")"
        );

        tenv.executeSql("insert into output_kafka select * from "+ etlResult);

        //TODO 4.execute
        env.execute();

    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;//事件时间
    }
}
//准备kafka主题
//kafka-topics --create --zookeeper node1:2181 --replication-factor 2 --partitions 3 --topic input_kafka
//kafka-topics --create --zookeeper node1:2181 --replication-factor 2 --partitions 3 --topic output_kafka
//kafka-console-producer --broker-list node1:9092 --topic input_kafka
//{"user_id": "1", "page_id":"1", "status": "success"}
//{"user_id": "1", "page_id":"1", "status": "success"}
//{"user_id": "2", "page_id":"1", "status": "success"}
//{"user_id": "4", "page_id":"1", "status": "success"}
//{"user_id": "1", "page_id":"1", "status": "fail"}
//kafka-console-consumer --bootstrap-server node1:9092 --topic output_kafka --from-beginning