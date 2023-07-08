package com.atguigu.Transform;

import com.atguigu.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 课程5：reduce聚合计算
 * 样例功能：统计每个用户的访问频次，然后找到最大的访问频次
 */
public class TransformReduceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从元素读取数据，这里避免了第二步中自己创建集合，测试代码更常用
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Tom", "./link?id=1", 2000L),
                new Event("Jack", "./baidu/123", 3000L),
                new Event("Tom", "./link?id=2", 2500L),
                new Event("Tom", "./link?id=3", 2700L),
                new Event("Tom", "./link?id=4", 3500L),
                new Event("Mary", "./home2", 1100L),
                new Event("Mary", "./home3", 1300L),
                new Event("Mary", "./home4", 1900L),
                new Event("Jack", "./baidu/456", 3500L),
                new Event("Jack", "./baidu/789", 4000L)
        );
        // 1.统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        }).keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            // 注意：reduce参数中，value1是之前的reduce规约结果，value2最新一个元素的值
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        // 2.根据当前用户点击的次数，选举出最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByUser.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        });
        result.print();
        env.execute();
    }
}
