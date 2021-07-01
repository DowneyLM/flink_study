package cn.avengers.connector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ConnectorDemo_JDBC {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        //3.transformation
        //4.sink
        DataStreamSink<Student> ds = env.fromElements(new Student(null, "ironman", 28))
                .addSink(JdbcSink.sink("INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (null, ?, ?)",
                        (ps, s) -> {
                            ps.setString(1, s.getName());
                            ps.setInt(2, s.getAge());
                        }
                        , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://node1:3306/bigdata")
                                .withUsername("root")
                                .withPassword("123456")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .build()));

//        env.fromElements(new Student(null,"ironman2",2012))
//                .addSink(JdbcSink.sink("INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (null, ?, ?)",
//                        new JdbcStatementBuilder<Student>() {
//                            @Override
//                            public void accept(PreparedStatement preparedStatement, Student student) throws SQLException {
//                                preparedStatement.setString(1, student.getName());
//                                preparedStatement.setInt(2, student.getAge());
//                            }
//                        }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                                .withUrl("jdbc:mysql://node1:3306/bigdata")
//                                .withUsername("root")
//                                .withPassword("123456")
//                                .withDriverName("com.mysql.jdbc.Driver")
//                                .build()))
//                ;


        //5.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student{
        private Integer id;
        private String name ;
        private Integer age;
    }

}
