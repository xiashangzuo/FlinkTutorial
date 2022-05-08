package Chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class richMapFunction {
    //  富函数

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        clicks.map(new RichMapFunction<Event, Long>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //  做一些初始化工作
                //  例如建立一个和MySQL的连接
                System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
            }

            @Override
            public Long map(Event value) throws Exception {
                //  对数据库进行读写
                return value.timestamp;
            }


            @Override
            public void close() throws Exception {
                //  清理工作,关闭和MySQL数据库的连接

                super.close();
                System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
            }
        }).print();

        env.execute();

    }
}
