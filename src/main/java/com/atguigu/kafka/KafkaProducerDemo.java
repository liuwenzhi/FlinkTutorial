package com.atguigu.kafka;

import com.atguigu.pojo.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.Logger;

/**
 * 本地环境测试kafka生产者代码，可以先在服务器上任意一个节点开启kafka消费，然后运行
 * 代码main方法，在服务器上查看消费的数据信息
 */
import java.util.Properties;

public class KafkaProducerDemo {
    private static Logger logger = Logger.getLogger(KafkaProducerDemo.class);

    private static Producer<Integer, String> producer = null;

    /*kafka集群配置，开kerberos环境的，使用局域网地址，使用单独的开kerberos的kafka组件*/
    private static String SERVERS = "192.168.190.100:9092";
    private static String BROKER_LIST = "192.168.190.100:9092";
    private static String TOPIC_NAME = "test";

    private KafkaProducerDemo() {
    }

    private static Producer<Integer, String> getInstance() {
        if (producer == null) {
            producer = createProducer();
        }
        return producer;
    }

    private static Producer<Integer, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", SERVERS);
        props.put("metadata.broker.list", BROKER_LIST);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        return producer;
    }

    public static void sendMsg(String msg) {
        // 发送数据到kafka,topic
        sendMsg(msg, TOPIC_NAME);
    }

    public static void sendMsg(String msg, String topicVal) {
        // 发送数据到kafka
        Producer<Integer, String> producer = getInstance();
        producer.send(new ProducerRecord<Integer, String>(topicVal, msg), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(metadata.offset());//1
                }
            }
        });
        producer.flush();// 所有缓存记录被立刻发送
    }

    public static void main(String[] args) throws Exception {
        // 模拟Event的数据记录
        /*for (int i = 0; i < 50; i++) {
            logger.info("输出第：" + i + "条记录");
            Event event = new Event("张三" + i, "https://localhost:777" + i, 1000L);
            sendMsg(event.toString());
            Thread.sleep(3000);
        }*/
        for (int i = 0; i < 10; i++) {
            sendMsg("hello world");
            Thread.sleep(1000);
        }
        for (int i = 10; i < 20; i++) {
            sendMsg("hello key");
            Thread.sleep(1000);
        }
    }
}
