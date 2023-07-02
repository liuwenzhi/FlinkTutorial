package com.atguigu.Transform;

import com.atguigu.pojo.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 课程3：测试flatmap算子,扁平映射，相对于map来说，多了一个扁平化的操作内容
 * 可以把部分数据按照某种规则打散，然后再做map这个一对一映射。
 * 注意一个点：单一抽象方法的接口，都可以使用lambda表达式实现，本节包中涉及到的几个Function接口都满足这个条件
 */
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从元素读取数据，这里避免了第二步中自己创建集合，测试代码更常用
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Tom", "./link?id=123", 2000L),
                new Event("Jack", "./baidu/123", 3000L)
        );
        // 通过自定义静态内部类实现
        stream.flatMap(new MyFlatMap()).print("1");

        // 通过lambda表达式实现
        stream.flatMap((Event event, Collector<String> collector) -> {
            if (event.user.equals("Mary")) {
                collector.collect(event.user);
                collector.collect(event.url);
            } else if (event.user.equals("Jack")) {
                collector.collect(event.url);
                collector.collect(event.timestamp.toString());
            } else {
                collector.collect(event.user);
                collector.collect(event.url);
                collector.collect(event.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {
        }).print("2");
        env.execute();
    }

    /**
     * 自定义一个FlatMap静态内部类接口，将输入的Event对象，做扁平化打散处理，输出String类型字符串
     * 拆开，打散，再做转换
     */
    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }
}
