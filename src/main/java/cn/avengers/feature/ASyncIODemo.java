package cn.avengers.feature;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 使用异步IO的先决条件
 * 1.数据库 (或kv存储)提供支持异步请求的client。
 * 2.没有异步请求客户端的话也可以将同步客户端丢到线程池中执行作为异步客户端。
 */
public class ASyncIODemo {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.source
        //数据源只有id
        //DataStreamSource[1,2,3,4,5]
        DataStreamSource<CategoryInfo> categoryDS = env.addSource(new RichSourceFunction<CategoryInfo>() {

            private boolean flag = true;

            @Override
            public void run(SourceContext<CategoryInfo> sourceContext) throws Exception {
                Integer[] ids = {1, 2, 3, 4, 5};
                for (Integer id : ids) {
                    sourceContext.collect(new CategoryInfo(id, null));
                }
            }

            @Override
            public void cancel() {
                this.flag = false;
            }
        });

        //3.Transformation

        //方式1：Java-vertx中提供的异步client实现异步IO
        //unorderedWait无序等待
        SingleOutputStreamOperator<CategoryInfo> result1 = AsyncDataStream
                .unorderedWait(categoryDS, new AsyncIOFuction1(), 1000, TimeUnit.SECONDS, 10);

        //方式2：MySQL中同步client+线程池模拟异步IO
        //unoderedWait无序等待
        SingleOutputStreamOperator<CategoryInfo> result2 = AsyncDataStream
                .unorderedWait(categoryDS, new AsyncIOFuction2(), 1000, TimeUnit.SECONDS, 10);

        //4.sink
        result1.print("方式1：Java-vertx中提供的异步client实现异步IO \n");
        result2.print("方式2：MYSQL中同步client + 线程池模拟异步IO \n");

        //5.excute
        env.execute();


    }

}



@Data
@AllArgsConstructor
@NoArgsConstructor
class CategoryInfo{
    private Integer id;
    private String name;
}

//MYSQL本身的客户端-需要把它变成支持异步的客户端：使用Vertx或者线程池
class MySQLSyncClient{

    private static transient Connection connection;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://node1:3306/bigdata";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";

    static {
        init();
    }

    private static void init() {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found!" + e.getMessage());
        }
        try {
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            System.out.println("init connection failed!" + e.getMessage());
        }
    }

    public void close(){
        try {
            if(connection != null){
                connection.close();
            }
        }catch (SQLException throwables) {
            System.out.println("close connection failed!" + throwables.getMessage());
        }
    }

    public CategoryInfo query(CategoryInfo category){
        try {
            String sql = "select id,name from  t_category where id = " + category.getId();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            if(rs!=null && rs.next()){
                category.setName(rs.getString("name"));
            }

        }catch (SQLException e){
            System.out.println("query failed" + e.getMessage());
        }
        return category;
    }

}

/**
 * 方式1：Java-vertx中提供的异步client实现异步io
 */
class AsyncIOFuction1 extends RichAsyncFunction<CategoryInfo,CategoryInfo>{

    private transient SQLClient mySQLClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        JsonObject mySQLClientConfig = new JsonObject();
        mySQLClientConfig
                .put("driver_class", "com.mysql.jdbc.Driver")
                .put("url", "jdbc:mysql://node1:3306/bigdata")
                .put("user", "root")
                .put("password", "123456")
                .put("max_pool_size", 20);

        VertxOptions options = new VertxOptions();
        options.setEventLoopPoolSize(10);
        options.setWorkerPoolSize(20);
        Vertx vertx = Vertx.vertx(options);
        //根据上面的配置参数获取异步请求的客户端
        mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);

    }

    @Override
    public void asyncInvoke(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) {

        mySQLClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
            @Override
            public void handle(AsyncResult<SQLConnection> event) {
                if(event.failed()){
                    return;
                }
                SQLConnection connection = event.result();
                connection.query("select id,name from t_category where id = " + input.getId(),
                        new Handler<AsyncResult<io.vertx.ext.sql.ResultSet>>() {
                            @Override
                            public void handle(AsyncResult<io.vertx.ext.sql.ResultSet> event) {
                                if(event.succeeded()){
                                    List<JsonObject> rows = event.result().getRows();
                                    for (JsonObject jsonObject : rows) {
                                        CategoryInfo categoryInfo = new CategoryInfo(jsonObject.getInteger("id"), jsonObject.getString("name"));
                                        resultFuture.complete(Collections.singletonList(categoryInfo));
                                    }
                                }
                            }
                        });
            }
        });
    }

    @Override
    public void close() throws Exception {
        mySQLClient.close();
    }

    @Override
    public void timeout(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) throws Exception {
        System.out.println("async call time out!");
        input.setName("未知");
        resultFuture.complete(Collections.singleton(input));
    }

}

/**
 * 方式2：同步调用 + 线程池模拟异步IO
 */

class AsyncIOFuction2 extends RichAsyncFunction<CategoryInfo,CategoryInfo>{

    private transient MySQLSyncClient client;
    private ExecutorService executorService;//线程池

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new MySQLSyncClient();
        executorService = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }

    @Override
    public void asyncInvoke(CategoryInfo categoryInfo, ResultFuture<CategoryInfo> resultFuture) throws Exception {

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                resultFuture.complete(Collections.singletonList((CategoryInfo) client.query(categoryInfo)));
            }
        });
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void timeout(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) throws Exception {
        System.out.println("async call time out!");
        input.setName("未知");
        resultFuture.complete(Collections.singleton(input));
    }
}




























