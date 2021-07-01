package cn.avengers.state;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;

/**
 * Author ZengZihang
 * Desc 使用KeyState中的ValueState获得流数据中的最大值/实际中可以使用maxBy即可
 */
public class StateDemo02_OperatorState {

    public static void main(String[] args) throws Exception {

        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);//并行度设置为1方便观察

        //下面的Checkpoint和重启策略
        env.enableCheckpointing(1000);//每隔1秒进行一次checkpoint
        env.setStateBackend(new FsStateBackend("file:///F:/2021 SoftWare/ckp"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //固定延迟重启策略：程序出现异常的时候，重启2次，每次延迟3秒种重启，超过2次，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        //TODO 1.source
        DataStream<String> ds = env.addSource(new MyKafkaSource()).setParallelism(1);

        //TODO 2.transformation


        //TODO 3. sink
        ds.print();

        //TODO 4.execute
        env.execute();

    }

    //使用OperatorState中的ListState模拟KafkaSource进行offset维护
    public static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction{

        private boolean flag = true;

        //-1.声明ListState
        private ListState<Long> offsetState = null; // 用来存放offset
        private Long offset = 0L ;// 用来存放offset的值

        //-2.初始化/创建ListState
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>("offsetState", Long.class);
            offsetState = context.getOperatorStateStore().getListState(stateDescriptor);
        }

        //-3.使用State
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {

            while (flag){
                Iterator<Long> iterator = offsetState.get().iterator();
                if(iterator.hasNext()){
                    offset = iterator.next();
                }
                offset += 1;
                int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
                sourceContext.collect("subTaskId" + subTaskId + ",当前的offset的值为：" + offset);
                Thread.sleep(1000);

                //模拟异常
                if(offset % 5 == 0){
                    System.out.println("bug出现了！");
                    throw new Exception("bug出现了！");
                }
            }
        }

        //-4.state持久化
        //该方法会定时执行将state状态从内存存入到checkpoint磁盘目录中
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            offsetState.clear();//清理内容数据并且存入checkpoint磁盘目录中
            offsetState.add(offset);
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
