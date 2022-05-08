package Chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class customPartitionTest {
    public static void main(String[] args) throws Exception {
        //  自定义一个分区器

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8).partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % 3;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return value;
            }
        }).print().setParallelism(3);

        env.execute();
    }
}
