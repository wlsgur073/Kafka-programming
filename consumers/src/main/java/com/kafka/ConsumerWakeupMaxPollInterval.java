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

public class ConsumerWakeupMaxPollInterval {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWakeupMaxPollInterval.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_02");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "30000"); // 30초 동안 poll() 메서드가 호출되지 않으면 consumer group rebalance가 발생

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topicName));

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook");
            consumer.wakeup(); // poll() 메서드가 블록되어 있는 상태에서 빠져나와서 InterruptedException을 발생시킴.

            try {
                mainThread.join(); // main thread가 종료될 때까지 대기
            } catch (InterruptedException e) {
                logger.error("InterruptedException occurred");
            }
        }));

        int loopCount = 0;
        try {
            while (true) {
                consumer.poll(Duration.ofMillis(1000))
                        .forEach(record ->
                                logger.info("key: {}, partition: {},  offset: {}, value: {}", record.key(), record.partition(), record.offset(), record.value())
                        );
                try {
                    // MAX_POLL_INTERVAL_MS_CONFIG 시간동안 poll() 메서드가 호출되지 않으면 consumer group rebalance가 발생
                    // consumer group rebalance가 발생하면 다시 loop를 돌기에 또 다시 poll()을 호출한다.
                    // 결국 cunsumer poll timeout has expired 가 발생하고 rebalance를 하게 된다. 의도치 않게 이런 상황이 발생하면 안되기에 주의해야 한다.
                    // 프로듀서는 계속 메시지를 보내기에, 컨슈머는 계속 메시지를 받아야 한다. 만약 해당 로직에서 병목 현상이 발생한다면, 로직을 최적화 하던가, 파티션을 늘려 병렬 처리를 더욱 분산시킬 수 있다.
                    logger.info("main thread is sleeping {} ms during while", loopCount*10000);
                    Thread.sleep(loopCount*10000);
                } catch (InterruptedException e) {
                    e.printStackTrace(); // for test
                }
            }
        } catch (WakeupException e) {
            logger.error("WakeupException occurred");
        } finally {
            logger.info("Closing consumer");
            consumer.close();
        }

    }
}