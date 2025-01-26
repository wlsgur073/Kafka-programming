package com.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);
    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();
    private String customKey;

    @Override
    public void configure(Map<String, ?> configs) {
        customKey = configs.get("custom.key").toString();// props에 설정한 custom.key 값을 가져올 수 있음.
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
//        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);// topic에 대한 partition 정보를 가져올 수 있음.
        int numPartitions = cluster.partitionCountForTopic(topic);// topic에 대한 partition 수를 가져올 수 있음.
        int numCustomPartitions = (int) (numPartitions * 0.5); // 5개의 파티션 중 2개의 파티션을 사용할 수 있도록 설정
        int partitionIndex = 0;

        if (keyBytes == null) {
            return this.stickyPartitionCache.partition(topic, cluster);
//            throw new InvalidRecordException("We expect all messages to have customer name as key"); // key가 없을 경우 예외 처리할수도 있음.
        }

        if (String.valueOf(key).equals(customKey)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numCustomPartitions; // 0, 1 중 하나의 파티션에 저장
        } else {
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numCustomPartitions) + numCustomPartitions; // 2, 3, 4 중 하나의 파티션에 저장
        }

        logger.info("key: {}, partitionIndex: {}", key.toString(), partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }
}
