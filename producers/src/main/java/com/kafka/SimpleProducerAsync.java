package com.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducerAsync {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class);

    public static void main(String[] args) {
        String topicName = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create KafkaProducer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // create network thread by KafkaProducer

        // Create ProducerRecord object // The generic type parameters must match.
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,  "Hello Async World!");

        // send message KafkaProducer
        producer.send(producerRecord, new Callback() { // you can replace lambda
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) { // from sender thread
                if (e == null) {
                    logger.info("offset = {}, partition = {}", recordMetadata.offset(), recordMetadata.partition());
                } else {
                    logger.error("exception error from broker: {}", e.getMessage());
                }
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            logger.error("InterruptedException: {}", e.getMessage());
        }

        producer.close();
    }
}