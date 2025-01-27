package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeup {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWakeup.class);

    public static void main(String[] args) {

        String topicName = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01"); // Java에서는 group.id를 명시해줘야 함.
//        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // default: latest, `earliest` means from the beginning
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topicName));

        Thread mainThread = Thread.currentThread(); // 현재 메인 스레드

        // main thread가 종료되면 shutdown hook이 실행됨.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook");
            consumer.wakeup(); // poll() 메서드가 블록되어 있는 상태에서 빠져나와서 InterruptedException을 발생시킴.

            try {
                mainThread.join(); // main thread가 종료될 때까지 대기
            } catch (InterruptedException e) {
                logger.error("InterruptedException occurred");
            }
        }));

        try {
            while (true) {
                consumer.poll(Duration.ofMillis(1000))
                        .forEach(record -> {
                            logger.info("key: {}, value: {}, offset: {}", record.key(), record.value(), record.offset());
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