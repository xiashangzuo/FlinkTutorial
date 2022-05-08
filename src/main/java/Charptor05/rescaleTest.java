package Charptor05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class rescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 0; i < 8; i++) {
                    //  将奇数发送到索引为1的并行子任务
                    //  将偶数发送到索引为0的并行子任务

                    if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
//                        System.out.println("当前子任务索引：" + getRuntimeContext().getIndexOfThisSubtask());
                        sourceContext.collect(i + 1);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
//                .rescale()
                .rebalance()
                .print().setParallelism(4);

        env.execute();
    }
}
