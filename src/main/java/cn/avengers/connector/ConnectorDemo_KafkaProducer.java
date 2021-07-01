package cn.avengers.connector;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Author ZengZihang
 * Desc
 * 使用自定义sink-官方提供的flink-connector-kafka_2.12-将数据保存到Kafka
 */
public class ConnectorDemo_KafkaProducer {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.Source
        DataStreamSource<Student> studentDS = env.fromElements(new Student(1, "tony", 19));

        //3.Transformations
        //注意：目前来说我们使用kafka使用的序列化和反序列化都是直接使用最简单的字符串，所以先将student转为字符串
        //可以直接调用Student的toString，也可以转为json
        SingleOutputStreamOperator<String> jsonDS = studentDS.map(new MapFunction<Student, String>() {
            @Override
            public String map(Student student) throws Exception {
                return JSON.toJSONString(student);
            }
        });

        //4.Sink
        jsonDS.print();

        //根据参数创建KafkaProducer/kafkaSink
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "node1:9092");
        FlinkKafkaProducer<String> flink_kafka = new FlinkKafkaProducer<>("flink_kafka", new SimpleStringSchema(), pros);
        jsonDS.addSink(flink_kafka);

        //5.execute
        env.execute();

        // /export/server/kafka/bin/kafka-console-consumer --bootstrap-server node1:9092 --topic flink_kafka
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }


}
