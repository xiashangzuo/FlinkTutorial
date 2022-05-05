import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class batchWordCount {
    /*
        第一个Flink代码：批处理 wordCount
        主要注意代码的执行环境和数据读取
     */
    public static void main(String[] args) throws Exception {

        //  创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //  读取文件
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {

            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));//  需要显式声明Tuple的类型

        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        /*
           不显式声明返回值会导致错误
         */
//        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
//            String[] words = line.split(" ");
//            for (String word : words
//            ) {
//                out.collect(Tuple2.of(word, 1L));
//            }
//        });
//
//        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
//
//        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        sum.print();


    }
}
