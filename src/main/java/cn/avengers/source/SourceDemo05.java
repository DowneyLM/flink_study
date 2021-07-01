package cn.avengers.source;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Author ZengZihang
 * Desc
 * 实际开发中,经常会实时接收一些数据,要和MySQL中存储的一些规则进行匹配,那么这时候就可以使用Flink自定义数据源从MySQL中读取数据
 * 那么现在先完成一个简单的需求:
 * 从MySQL中实时加载数据
 * 要求MySQL中的数据有变化,也能被实时加载出来
 */
public class SourceDemo05 {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStreamSource<Student> studentDS = env.addSource(new MYSQLSource()).setParallelism(1);

        //3.Transformation

        //4.sink
        studentDS.print();

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

    public static class MYSQLSource extends RichParallelSourceFunction<Student>{

        private Connection conn = null;
        private PreparedStatement ps = null;
        private boolean flag = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            //加载驱动，开启连接
            conn = DriverManager.getConnection("jdbc:mysql://node1:3306/bigdata", "root", "123456");
            String sql = "select id,name,age from t_student";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<Student> sourceContext) throws Exception {

            while (flag){
                ResultSet rs = ps.executeQuery();
                while (rs.next()){
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    int age = rs.getInt("age");
                    sourceContext.collect(new Student(id,name,age));
                }
                TimeUnit.SECONDS.sleep(5);
            }

        }

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void close() throws Exception {
            if(conn!=null) conn.close();
            if(ps!=null) ps.close();
        }
    }

}
