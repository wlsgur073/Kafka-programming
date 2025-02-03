package com.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);

    private KafkaProducer<String, String> producer;
    private String topicName;
    private boolean isAsync;

    public FileEventHandler(KafkaProducer<String, String> producer, String topicName, boolean isAsync) {
        this.producer = producer;
        this.topicName = topicName;
        this.isAsync = isAsync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws ExecutionException, InterruptedException {
        // 메시지를 하나씩 보냄
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvent.key, messageEvent.value);

        if (!this.isAsync) {
            RecordMetadata recordMetadata = this.producer.send(producerRecord).get();
            logger.info("offset = {}, partition = {}", recordMetadata.offset(), recordMetadata.partition());
        } else {
            this.producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("offset = {}, partition = {}", recordMetadata.offset(), recordMetadata.partition());
                } else {
                    logger.error("exception error from broker: {}", e.getMessage());
                }
            });
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        boolean isAsync = false;

        FileEventHandler fileEventHandler = new FileEventHandler(producer, topicName, isAsync);
        MessageEvent messageEvent = new MessageEvent("key00001", "test massage");
        fileEventHandler.onMessage(messageEvent);
    }

}
