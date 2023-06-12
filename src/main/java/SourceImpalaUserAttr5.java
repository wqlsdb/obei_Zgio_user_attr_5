import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class SourceImpalaUserAttr5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(10);
        System.setProperty("HADOOP_USER_NAME","obeiadmin");
        //todo:开启检查点   1000是1S这里是6min
        env.enableCheckpointing(10000); //生产环境设置分钟级
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // 设置checkpoint的超时时间 即一次checkpoint必须在该时间内完成 不然就丢弃
        env.getCheckpointConfig().setCheckpointTimeout(360000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //当 Flink 任务取消时，保留外部保存的 checkpoint 信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置使用非对齐检查点。它可以极大减少背压情况下检查点的时间，但是只有在精确一次的检查点并且允许的最大检查点并发数量为1的情况下才能使用
        //env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/checkpoint/impala_user_attrcluster1");
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //todo:设置延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                300, // 尝试重启的次数
                Time.of(30,TimeUnit.SECONDS) // 间隔
        ));
        MyImpalaRichSource myImpalaRichSource = new MyImpalaRichSource();
        DataStreamSource<UserBean> userBeanDataStreamSource = env.addSource(myImpalaRichSource);
        WriteHuid.extracted(env,userBeanDataStreamSource);
        //MyImpalaRichSourceTest myImpalaRichSourceTest = new MyImpalaRichSource();
/*        DataStreamSource<UserBean> userBeanDataStreamSource = env.addSource(myImpalaRichSource);
        userBeanDataStreamSource.print();
        env.execute();*/

    }
}
