package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SimpleConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args) {

        String topicName = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "simple-group");
//        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000"); // default 3000
//        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "90000"); // default 45000
//        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000"); // default 300000

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topicName)); // 브로커에 topic을 구독하도록 설정

        // 지속적으로 polling을 통해 메시지를 가져옴
        while (true) {
            consumer.poll(Duration.ofMillis(1000))
                    .forEach(record -> {
                        logger.info("key: {}, value: {}", record.key(), record.value());
                    });
        }

//        consumer.close();
    }
}