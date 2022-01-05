package com.jx.kafak.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Author jx
 * @Date 2021/12/30
 */
public class KafkaProducerDemo {
    public static final String brokerList = "192.168.220.20:9092";
    public static final String topic = "topic-demo";

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer producer = new KafkaProducer(properties);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,"Kafka消费者多线程实现");
        try {
            producer.send(producerRecord);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
