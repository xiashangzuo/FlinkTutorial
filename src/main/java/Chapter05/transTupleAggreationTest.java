package Chapter05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class transTupleAggreationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("a", 2),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

//        System.out.println("求和");
//        stream.keyBy(e -> e.f0).sum(1).print();
//        stream.keyBy(e -> e.f0).sum("f1").print();

//        System.out.println("求最大值");
//        stream.keyBy(e -> e.f0).max("f1").print();

        System.out.println("最大值条目的全部信息:");
        stream.keyBy(e -> e.f0).maxBy(1).print();
//
//        System.out.println("最小值条目的全部信息：");
//        stream.keyBy(e -> e.f0).minBy(1).print();

        env.execute();
    }
}
