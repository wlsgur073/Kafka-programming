package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMTopicRebalanced {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerMTopicRebalanced.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()); // default: RangeAssignor

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2")); // two topics

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook");
            consumer.wakeup();

            try {
                mainThread.join(); // main thread가 종료될 때까지 대기
            } catch (InterruptedException e) {
                e.printStackTrace(); // for test
            }
        }));

        try {
            while (true) {
                consumer.poll(Duration.ofMillis(1000))
                        .forEach(record -> {
                            logger.info("topic: {}, key: {}, partition: {},  offset: {}, value: {}"
                                    , record.topic(), record.key(), record.partition(), record.offset(), record.value());
                        });
            }
        } catch (WakeupException e) {
            logger.error("WakeupException occurred");
        } finally {
            logger.info("Closing consumer");
            consumer.close();
        }

    }
}