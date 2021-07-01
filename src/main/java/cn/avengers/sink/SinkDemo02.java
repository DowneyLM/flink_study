package cn.avengers.sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * Author ZengZihang
 * Desc
 * 1.ds.print 直接输出控制台
 * 2.ds.printToErr() 直接输出到控制台，用红色
 * 3.ds.collect() 将分布式数据收集为本地集合
 * 4.ds.setParallelism(1).writeAsText("本地/HDFS的path","WriteMode.OverWRITE")
 */
public class SinkDemo02 {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStream<Student> ds = env.fromElements(new Student(null, "capation", 29));


        //3.transformation
        //4.sink
        ds.addSink(new MySQLSink());

        //5.execute
        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Student{
        private Integer id;
        private String name;
        private Integer age;
    }

    public static class MySQLSink extends RichSinkFunction<Student>{

        private Connection conn = null;
        private PreparedStatement ps = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //加载驱动，开启连接
            //Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://node1:3306/bigdata", "root", "123456");
            String sql = "INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (null, ?, ?)";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void invoke(Student value, Context context) throws Exception {
            //给ps中的？设置具体值
            ps.setString(1,value.getName());
            ps.setInt(2, value.getAge());
            //执行sql
            ps.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
        }
    }

}
