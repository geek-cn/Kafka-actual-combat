package com.jx.kafak.consumer;

import com.jx.kafak.producer.Company;
import com.jx.kafak.protostuff.ProtostuffDeserialzer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author jx
 * @Date 2021/12/27
 */
public class KafkaConsumerAnalysis {
    public static final String brokerList = "192.168.220.20:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ProtostuffDeserialzer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(properties);
        List<String> topicList = Arrays.asList(topic);
        consumer.subscribe(topicList);

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(1000));
                // for (ConsumerRecord<String, Company> record : records) {
                //     System.out.println("topic=" + record.topic() + "; partion=" + record.partition() + "; key="
                //             + record.key() + "; value=" + record.value());
                // }

                /**
                 * 分区维度消费
                 */
                for (TopicPartition partition : records.partitions()) {
                    for (ConsumerRecord<String, Company> record : records.records(partition)) {
                        System.out.println("分区维度消费" + record.key() + ":" + record.value());
                    }
                }
                /**
                 * 主题维度消费
                 */
                for (String topic : topicList) {
                    for (ConsumerRecord<String, Company> record : records.records(topic)) {
                        System.out.println("主题维度消费" + record.key() + ":" + record.value());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }

}
