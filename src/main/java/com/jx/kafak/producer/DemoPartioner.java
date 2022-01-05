package com.jx.kafak.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author jx
 * @Date 2021/12/24
 * 自定义分区器
 */
public class DemoPartioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger(0);
    @Override
    public int partition(String topic, Object key, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int partionsNum = partitions.size();
        if (bytes == null) {
            return counter.getAndIncrement() % partionsNum;
        } else {
            return Utils.toPositive(Utils.murmur2(bytes)) % partionsNum;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
