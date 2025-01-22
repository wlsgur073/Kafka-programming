package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class);

    public static void main(String[] args) {
        String topicName = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create KafkaProducer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // create network thread by KafkaProducer

        // Create ProducerRecord object // The generic type parameters must match.
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "Hello Sync World!");

        // send message KafkaProducer
        try {
            RecordMetadata recordMetadata = producer.send(producerRecord).get();
            logger.info("offset = {}, partition = {}", recordMetadata.offset(), recordMetadata.partition());
        } catch (InterruptedException e) { // thread
            logger.error("InterruptedException: {}", e.getMessage());
        } catch (ExecutionException e) { // concurrency
            logger.error("ExecutionException: {}", e.getMessage());
        } finally {
            producer.close(); // auto flush
        }
    }
}