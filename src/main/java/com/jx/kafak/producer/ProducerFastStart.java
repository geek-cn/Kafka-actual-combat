package com.jx.kafak.producer;

import com.jx.kafak.consumer.ConsumerInterceptorTTL;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author jx
 * @Date 2021/12/22
 */
public class ProducerFastStart {
    public static final String brokerList = "192.168.220.20:9092";
    public static final String topic = "topic-demo";
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                 ProducerInterceptorPrefixPlus.class.getName()  + "," + ProducerInterceptorPrefix.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        // ProducerRecord<String,String> record = new ProducerRecord<>(topic,"hello kafka");
        try {
            ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, 0, System
                    .currentTimeMillis() - EXPIRE_INTERVAL, null,"first-expire-data");
            producer.send(record1).get();
            ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, 0, System
                    .currentTimeMillis(), null,"normal-data");
            producer.send(record2).get();
            ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, 0, System
                    .currentTimeMillis() - EXPIRE_INTERVAL, null,"last-expire-data");
            producer.send(record3).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();

        }

    }
}
