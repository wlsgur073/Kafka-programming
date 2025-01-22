package com.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncWithKey {
    private static final Logger logger = LoggerFactory.getLogger(ProducerAsyncWithKey.class);

    public static void main(String[] args) {
        String topicName = "multipart-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create KafkaProducer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // create network thread by KafkaProducer

        for (int i = 0; i < 20; i++) {
            // Create ProducerRecord object
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topicName, String.valueOf(i),  "Hello World! seq: " + i);

            // send message KafkaProducer
            producer.send(producerRecord, (metadata, e) -> {
                if (e == null) {
                    logger.info("offset = {}, partition = {}", metadata.offset(), metadata.partition());
                } else {
                    logger.error("exception error from broker: {}", e.getMessage());
                }
            });
        }


        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            logger.error("InterruptedException: {}", e.getMessage());
        }

        producer.close();
    }
}