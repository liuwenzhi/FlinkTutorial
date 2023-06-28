package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 无界数据流处理，实际的流处理代码
 * 注意：本项目执行的前提是：netcat必须要在node01这个服务器节点上启动，没有的话直接包括
 * 视频中给的命令是：nc -lk 7777，本地虚拟机上未执行成功
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过flink自定义的工具，从args参数里边获取入参信息
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // 参数信息：node01
        String hostname = parameterTool.get("host");
        // 参数信息：7777
        Integer port = parameterTool.getInt("port");
        // 2. 读取文本流
        DataStreamSource<String> lineDSS = env.socketTextStream(hostname,port);
        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING).map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 分 组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
                .keyBy(t -> t.f0);
        // 5. 求 和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
                .sum(1);
        // 6. 打 印
        result.print();
        // 7. 执 行
        env.execute();
    }
}

