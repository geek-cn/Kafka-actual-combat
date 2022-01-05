package com.jx.kafak.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * @Author jx
 * @Date 2021/12/30
 */
public class ThirdMultiConsumerThreadDemo {
    public static final String brokerList = "192.168.220.10:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";

    public static Properties initConfig() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        // prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        return prop;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        new KafkaConsumerThread(properties,topic,Runtime.getRuntime().availableProcessors()).start();
    }

    public static class KafkaConsumerThread extends Thread{
        KafkaConsumer<String,String> consumer ;
        int threadNumber;
        ExecutorService executorService;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        public KafkaConsumerThread(Properties properties,String topics,int threadNumber) {
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topics));
            this.threadNumber = threadNumber;
            this.executorService = new ThreadPoolExecutor(threadNumber,threadNumber,0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(100),new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (consumerRecords != null) {
                    executorService.submit(new RecordsHandler(consumerRecords,offsets));
                }
                synchronized (offsets) {
                    consumer.commitSync(offsets);
                }
            }
        }
    }

    public static class RecordsHandler extends Thread{
        private ConsumerRecords consumerRecords;
        private  Map<TopicPartition, OffsetAndMetadata> offsets;
        public RecordsHandler(ConsumerRecords consumerRecords, Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.consumerRecords = consumerRecords;
            this.offsets = offsets;
        }

        @Override
        public void run() {
                Set<TopicPartition> partitions = consumerRecords.partitions();
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String,String>> records = consumerRecords.records(partition);

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("topic=" + record.topic() + "; key=" + record.key() + "; value=" + record.value());
                    }

                    //当前消费位移
                    long lastConsumeOffset = records.get(records.size() -1).offset();
                    synchronized (offsets) {
                        if (!offsets.containsKey(partition)){
                            //消费位移未提交
                            offsets.put(partition,new OffsetAndMetadata(lastConsumeOffset + 1L));
                        } else {
                            //当前partition位移
                            long position = offsets.get(partition).offset();
                            if (position < lastConsumeOffset + 1) {
                                //当前partition消费位移小于当下一次将要提交的消费位移
                                offsets.put(partition,new OffsetAndMetadata(lastConsumeOffset + 1));
                            }
                        }
                    }
                }
        }
    }
}
