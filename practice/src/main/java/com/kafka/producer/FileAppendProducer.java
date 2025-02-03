package com.kafka.producer;

import com.kafka.event.EventHandler;
import com.kafka.event.FileEventHandler;
import com.kafka.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {
    private static final Logger logger = LoggerFactory.getLogger(FileAppendProducer.class);

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        boolean isAsync = false;

        File file = new File("${Absolute path}\\src\\main\\resources\\pizza_append.txt");
        EventHandler eventHandler = new FileEventHandler(producer, topicName, isAsync);
        FileEventSource fileEventSource = new FileEventSource(5000, file, eventHandler);

        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join(); // 해당 스레드가 종료될 때까지 main 스레드는 대기
        } catch (InterruptedException e) {
            logger.error("FileAppendProducer.InterruptedException: {}", e.getMessage());
        } finally {
            producer.close();
        }
    }
}
