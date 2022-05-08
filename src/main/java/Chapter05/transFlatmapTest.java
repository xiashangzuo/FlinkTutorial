package Chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class transFlatmapTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //  通过传入FlatMap类来实现
//        stream.flatMap(new FlatMap()).print();

        //  通过Lambda表达式来实现 需要声明返回类型
        stream.flatMap((Event event, Collector<String> out) -> {
            out.collect(event.url);
        }).returns(Types.STRING).print();


        env.execute();


    }

    public static class FlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            if (value.user.equals("Mary")) {
                out.collect(value.user);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        }
    }
}
