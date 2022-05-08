package Chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class transFilterTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        System.out.println("通过匿名内部类实现Filter：");
        stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Mary");
            }
        }).print();

        System.out.println("传入FilterFunction实现类:");
        stream.filter(new UserFilter()).print();

        env.execute();
    }

    public static class UserFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }

}
