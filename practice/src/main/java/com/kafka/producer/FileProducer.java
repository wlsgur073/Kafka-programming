package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {
    private static final Logger logger = LoggerFactory.getLogger(FileProducer.class);

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String filePath = "${Absolute path}\\src\\main\\resources\\pizza_sample.txt";

        sendFileMessage(producer, topicName, filePath);

        producer.close();
    }

    private static void sendFileMessage(KafkaProducer<String, String> producer, String topicName, String filePath) {
        String line;
        final String delimiter = ",";

        try (FileReader fileReader = new FileReader(filePath)){
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                // split the line by `delimiter` and `join()` the tokens with delimiter
                String joinStr = String.join(delimiter, tokens);
                value.append(joinStr);

                sendMassage(producer, topicName, key, value.toString());
            }

        } catch (IOException e) {
            logger.error("IOException: {}", e.getMessage());
        }
    }

    private static void sendMassage(KafkaProducer<String, String> producer, String topicName, String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key,  value);
        logger.info("key: {}, value: {}", key, value);

        // send message KafkaProducer
        producer.send(producerRecord, (metadata, e) -> {
            if (e == null) {
                logger.info("offset = {}, partition = {}", metadata.offset(), metadata.partition());
            } else {
                logger.error("exception error from broker: {}", e.getMessage());
            }
        });
    }
}