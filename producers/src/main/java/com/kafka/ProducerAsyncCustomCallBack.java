package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncCustomCallBack {
    private static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCallBack.class);

    public static void main(String[] args) {
        String topicName = "multipart-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create KafkaProducer object
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 20; i++) {
            CustomCallback customCallback = new CustomCallback(i); // To print which partition the callback belongs to.

            // Create ProducerRecord object
            ProducerRecord<Integer, String> producerRecord =
                    new ProducerRecord<>(topicName, i,  "Hello World! seq: " + i);

            // send message KafkaProducer
            producer.send(producerRecord, customCallback);
        }


        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            logger.error("InterruptedException: {}", e.getMessage());
        }

        producer.close();
    }
}