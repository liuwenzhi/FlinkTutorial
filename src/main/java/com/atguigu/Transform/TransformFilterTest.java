package com.atguigu.Transform;

import com.atguigu.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 课程2：测试filter算子
 * 包括三种实现方式，前两种方式可以参考map代码
 */
public class TransformFilterTest {
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
        // lambda表达式函数体式写法
        SingleOutputStreamOperator<Event> result = stream.filter(data -> {
            return data.user.equals("Mary");
        });
        result.print();
        // 使用lambda表达式另一种简单写法
        stream.filter(data -> data.user.equals("Tom")).print();
        stream.filter(data -> data.user.equals("Jack")).print("Jack lambda");
        env.execute();
    }
}
