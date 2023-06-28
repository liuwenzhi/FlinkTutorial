package com.atguigu.pojo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 文件源端测试，代码可用作测试数据使用，相当于把不同的源端数据，在本机idea环境中，
 * 放到本机内存中进行处理
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1、读取文本数据，相当于是批量的数据，即：有界的数据集合，然后做一个流式的打印输出，从文件中读取有界流数据是最常见的批处理方式
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");
        stream1.print("click");

        // 2、自定义集合数据类型作为源
        List<Integer> list = new ArrayList<Integer>() {
            {
                add(1);
                add(2);
                add(5);
            }
        };
        DataStreamSource<Integer> numStream = env.fromCollection(list);
        numStream.print("num");

        // 自定义pojo对象
        List<Event> events = new ArrayList<Event>() {
            {
                add(new Event("Mary", "./home", 1000L));
                add(new Event("Tom", "./link?id=123", 2000L));
                add(new Event("Jack", "./baidu/123", 3000L));
            }
        };
        DataStreamSource<Event> eventStream = env.fromCollection(events);
        eventStream.print("event");

        // 3、从元素读取数据，这里避免了第二步中自己创建集合，测试代码更常用
        DataStreamSource<Event> elementsStream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Tom", "./link?id=123", 2000L),
                new Event("Jack", "./baidu/123", 3000L)
        );
        elementsStream.print("element");

        env.execute();
    }
}
