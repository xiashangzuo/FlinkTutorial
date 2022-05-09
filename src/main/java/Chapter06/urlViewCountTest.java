package Chapter06;

import Chapter05.ClickSource;
import Chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
/*
        想要得到热门的 url，前提是得到每个链接的“热门度”。一般情况下，可以用
        url 的浏览量（点击量）表示热门度。我们这里统计 10 秒钟的 url 浏览量，每 5 秒钟更新一次；
        另外为了更加清晰地展示，还应该把窗口的起始结束时间一起输出。我们可以定义滑动窗口，
        并结合增量聚合函数和全窗口函数来得到统计结果
 */

public class urlViewCountTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.keyBy(data -> data.url)  //  根据网址分组
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))   //窗口长10s 滑动步长5s
                .aggregate(new urlViewCountAgg(), new urlViewCountResult())  // 同时传入增量聚合函数和全窗口函数
                .print();

        env.execute();
    }

    //  自定义增量聚合函数，来一条数据就加一
    private static class urlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    //  自定义窗口处理函数，只需要包装窗口信息
    private static class urlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {

            //  结合窗信息，包装输出内容
            long start = context.window().getStart();
            long end = context.window().getEnd();

            //  迭代器中只有一个元素，就是增量聚合函数的计算结果
            out.collect(new UrlViewCount(url,elements.iterator().next(),start,end));

        }
    }
}
