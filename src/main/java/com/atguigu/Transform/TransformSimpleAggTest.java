package com.atguigu.Transform;

import com.atguigu.pojo.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 课程4：简单聚合计算，按键分组之后进行简单聚合计算
 * 注意max算子和maxBy两个算子的区别，在比较的时候，如果出现比较属性值相等的情况，maxBy会拿最新的记录，max会用原来的记录。两个算子会用到不同的场景中
 */
public class TransformSimpleAggTest {
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
        // 统计当前用户的最后一次访问，提取当前用户最近一次访问数据，注意：这里还没有使用窗口，是来一条数据处理一次，所以看到实际输出结果有很多
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp").print("max");
        // 使用maxBy聚合算子实现统计功能
        stream.keyBy(data -> data.user).maxBy("timestamp").print("maxBy");
        env.execute();
    }
}
