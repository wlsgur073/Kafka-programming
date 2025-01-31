package com.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerCommit {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerCommit.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_03");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000"); // default 5000
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // default true

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topicName));

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook");
//            consumer.wakeup(); // 비정상 종료 시 auto commit이 발생

            try {
                mainThread.join(); // main thread가 종료될 때까지 대기
            } catch (InterruptedException e) {
                logger.error("InterruptedException occurred");
            }
        }));

//        pollAutoCommit(consumer);
        pollAutoCommitSync(consumer);
    }

    private static void pollAutoCommitSync(KafkaConsumer<String, String> consumer) {
        int loopCount = 0;
        try (consumer) {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                logger.info("###### loopCount: {}, consumerRecords count: {}", loopCount++, consumerRecords.count());
                consumerRecords.forEach(record ->
                        logger.info("key: {}, partition: {},  offset: {}, value: {}"
                                , record.key(), record.partition(), record.offset(), record.value()
                        )
                );

                try {
                    if (consumerRecords.count() > 0) {
                        consumer.commitSync(); // sync commit
                        logger.info("CommitSync success");
                    }
                } catch (CommitFailedException e) { // commit을 여러 번 재시도하다가 실패하면 exception 발생
                    logger.error("CommitFailedException occurred: {}", e.getMessage());
                }
            }
        } catch (WakeupException e) {
            logger.error("WakeupException occurred");
        } finally {
            logger.info("Closing consumer");
        }
    }

    private static void pollAutoCommit(KafkaConsumer<String, String> consumer) {
        int loopCount = 0;
        try (consumer) {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                logger.info("###### loopCount: {}, consumerRecords count: {}", loopCount++, consumerRecords.count());
                consumerRecords.forEach(record ->
                        logger.info("key: {}, partition: {},  offset: {}, value: {}"
                                , record.key(), record.partition(), record.offset(), record.value()
                        )
                );
                try {
                    logger.info("main thread is sleeping {} ms during while", 10000);
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace(); // for test
                }
            }
        } catch (WakeupException e) {
            logger.error("WakeupException occurred");
        } finally {
            logger.info("Closing consumer");
        }
    }
}