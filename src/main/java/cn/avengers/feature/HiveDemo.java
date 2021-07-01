package cn.avengers.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

/**
 * Author ZengZihang
 * Desc
 */
public class HiveDemo {

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        //TODO 指定hive的配置
        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "./conf";

        //TODO 根据配置创建hiveCatalog
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        //注册catalog
        tableEnv.registerCatalog("myhive", hive);
        //使用注册的catalog
        tableEnv.useCatalog("myhive");

        //向Hive表中写入数据
        //String insertSQL = "insert into person select * from person";
        //TableResult result = tableEnv.executeSql(insertSQL);
        String sql = "select 1,2,3,4";
        Table table = tableEnv.sqlQuery(sql);

        table.printSchema();

        DataStream<Tuple2<Boolean, Row>> result = tenv.toRetractStream(table, Row.class);

        result.print();

        env.execute();

        //System.out.println(result.getJobClient().get().getJobStatus());
    }
}