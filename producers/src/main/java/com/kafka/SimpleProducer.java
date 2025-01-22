package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer config setting
        // null, "Hello World!"

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create KafkaProducer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // Create ProducerRecord object // The generic type parameters must match.
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "key-001", "Hello World!");
        
        // send message KafkaProducer
        producer.send(producerRecord); // return ASK in Future from the broker

        producer.flush(); // flush buffer
        producer.close(); // auto flush // like same working Hibernate session flush and close
    }
}