package com.atguigu.pojo;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义数据源端，以clicks点击事件为标准。
 * 代码仅能作为测试样例使用
 */
public class ClickSource implements SourceFunction<Event> {

    private Boolean flag = true;

    /**
     * sourceContext对象是源任务的上下文
     */
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Tom", "Hans", "Alice", "Lina"};
        String[] urls = {"./home", "./link?id=123", "./baidu/123", "./google/456", "./alibaba/789", "./zhihu?page=123", "./sanlitun?sex=women"};
        while (flag) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user, url, timestamp));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
