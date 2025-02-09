package com.kafka.producer;

import com.kafka.model.OrderRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class OrderSerdeProducer {
    private static final Logger logger = LoggerFactory.getLogger(OrderSerdeProducer.class);

    public static void main(String[] args) {
        String topicName = "order-serde-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());

        KafkaProducer<String, OrderRecord> producer = new KafkaProducer<>(props);
        String filePath = "C:\\Users\\kim\\Desktop\\clone\\Kafka-project\\practice\\src\\main\\resources\\pizza_sample.txt";

        sendFileMessage(producer, topicName, filePath);

        producer.close();
    }

    private static void sendFileMessage(KafkaProducer<String, OrderRecord> producer, String topicName, String filePath) {
        String line;
        final String delimiter = ",";

        try (FileReader fileReader = new FileReader(filePath)){
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                OrderRecord orderRecord = new OrderRecord(tokens[1], tokens[2], tokens[3],
                        tokens[4], tokens[5], tokens[6], LocalDateTime.parse(tokens[7].trim(), formatter));

                sendMassage(producer, topicName, key, orderRecord);
            }

        } catch (IOException e) {
            logger.error("IOException: {}", e.getMessage());
        }
    }

    private static void sendMassage(KafkaProducer<String, OrderRecord> producer, String topicName, String key, OrderRecord value) {
        ProducerRecord<String, OrderRecord> producerRecord = new ProducerRecord<>(topicName, key, value);
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