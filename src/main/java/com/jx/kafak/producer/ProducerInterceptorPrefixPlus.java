package com.jx.kafak.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author jx
 * @Date 2021/12/24
 */
public class ProducerInterceptorPrefixPlus implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String modifyValue = "prefix2-" + producerRecord.value();
        return new ProducerRecord<>(producerRecord.topic(),producerRecord.partition(),producerRecord.timestamp(),
                producerRecord.key(),modifyValue,producerRecord.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
