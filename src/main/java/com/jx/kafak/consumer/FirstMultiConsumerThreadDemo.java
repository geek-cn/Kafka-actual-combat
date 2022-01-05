package com.jx.kafak.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Author jx
 * @Date 2021/12/30
 * 第一种多线程消费实现方式
 */
public class FirstMultiConsumerThreadDemo {
    public static final String brokerList = "192.168.220.10:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";

    public static Properties initConfig() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        return prop;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        int consumerThreadNum = 4;

        for (int i = 0; i < consumerThreadNum; i++) {
            new KafkaConsumerThread(properties,topic).start();
        }
    }

    public static class KafkaConsumerThread extends Thread{
        KafkaConsumer<String,String> consumer;
        public KafkaConsumerThread(Properties prop,String topics) {
            consumer = new KafkaConsumer<String, String>(prop);
            consumer.subscribe(Arrays.asList(topics));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        System.out.println("topic: " + record.topic() + ", key: " + record.key() + "; value: " + record.value());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }
    }
}
