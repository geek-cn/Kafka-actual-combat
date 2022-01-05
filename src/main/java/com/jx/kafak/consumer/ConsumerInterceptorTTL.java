package com.jx.kafak.consumer;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author jx
 * @Date 2021/12/29
 */
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String,String> {
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    /**
     * 对消息进行相应的定制化操作，比如修改返回的消息内容、按照某种规则过滤消息
     * poll() 方法返回之前调用
     * @param consumerRecords
     * @return
     */
    @Override
    public ConsumerRecords<String,String> onConsume(ConsumerRecords<String,String> consumerRecords) {
        long now = System.currentTimeMillis();
        Map<TopicPartition,List<ConsumerRecord<String,String>>> newRecords = new HashMap<>();
        for (TopicPartition partition : consumerRecords.partitions()) {
            List<ConsumerRecord<String, String>> records = consumerRecords.records(partition);
            List<ConsumerRecord<String,String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                if (now - record.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(partition,newTpRecords);
            }
        }

        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void close() {

    }

    /**
     * 记录跟踪所提交的位移信息
     * 提交完消费位移之后调用
     * @param map
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        map.forEach((tp,offset) -> {
            System.out.println(tp + ":" + offset);
        });
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
