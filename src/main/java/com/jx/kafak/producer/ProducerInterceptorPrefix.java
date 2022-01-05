package com.jx.kafak.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author jx
 * @Date 2021/12/24
 * 实现自定义生产者拦截器
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String,String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String modifyValue = "prefix1-" + producerRecord.value();
        return new ProducerRecord<>(producerRecord.topic(),producerRecord.partition(),producerRecord.timestamp(),
                producerRecord.key(), modifyValue,producerRecord.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            sendSuccess ++;
        } else {
            sendFailure ++;
        }
    }

    @Override
    public void close() {
         double successRatio = sendSuccess / (sendFailure + sendSuccess);
        System.out.println("发送成功率：  " + String.format("%f",successRatio * 100 ) + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
