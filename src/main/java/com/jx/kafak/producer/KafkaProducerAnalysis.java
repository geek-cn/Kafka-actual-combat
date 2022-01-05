package com.jx.kafak.producer;

import com.jx.kafak.protostuff.ProtostuffSerializer;
import org.apache.kafka.clients.producer.*;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * @Author jx
 * @Date 2021/12/23
 */
public class KafkaProducerAnalysis {
    public static final String brokerList = "192.168.220.20:9092";
    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                StringSerializer.class.getName());
        //使用自定义序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ProtostuffSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        //使用自定义分区器
       properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartioner.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);
        Company company = Company.builder().name("hiddenKafka").address("China").build();

        ProducerRecord<String,Company> record  = new ProducerRecord<>(topic,company);

        try {
            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(recordMetadata.topic() + recordMetadata.partition() + recordMetadata.offset());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
