package com.atguigu.Transform;

import com.atguigu.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试map算子
 */
public class TransformMapTest {
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
        // 1、使用自定义类，实现map接口
        SingleOutputStreamOperator<String> result = stream.map(new MyMapper());
        result.print();
        // 2、使用匿名类实现map function
        SingleOutputStreamOperator<String> result1 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });
        result1.print();
        // 3、传入lambda表达式，对于类似2这种只有一个方法的接口而言，可以使用lambda表达式替代，lambda表达式本身就是一个匿名函数表达式
        // lambda表达式本身就是一个向右的箭头，里边的data对象，识别到了匿名函数入参，即：stream定义的泛型类型
        SingleOutputStreamOperator<String> result2 = stream.map(data -> data.user);
        result2.print();
        env.execute();
    }

    // 自定义MapFunction，从Event对象中拆出user属性
    public static class MyMapper implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
