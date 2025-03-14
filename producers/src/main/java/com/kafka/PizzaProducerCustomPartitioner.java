package com.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducerCustomPartitioner {
    private static final Logger logger = LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class);


    public static void main(String[] args) {
        String topicName = "pizza-topic-partitioner";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.kafka.CustomPartitioner");
        props.setProperty("custom.key", "P001");

        // Create KafkaProducer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        sendPizzaMessage(topicName, producer,
                -1, 100, 0, 0, false
        );

        producer.close();
    }

    /**
     * @param iterCount A value of -1 means an infinite loop.
     * @param interIntervalMillis Interval for loop.
     * @param intervalCount Interval each loop count.
     * @param sync `true` is sync, `false` is async.
     * */
    private static void sendPizzaMessage(String topicName, KafkaProducer<String, String> producer,
                                         int iterCount, int interIntervalMillis, int intervalMillis,
                                         int intervalCount, boolean sync) {
        PizzaMessage pizzaMessage = new PizzaMessage();

        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterSeq++ != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    topicName, pMessage.get("key"), pMessage.get("message")
            );
            sendMessage(producer, producerRecord, pMessage, sync);

            // Take a sleep in while loop
            if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("######### IntervalCount : {} intervalMillis : {} #########", intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            // each
            if (interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis : {}", interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    private static void sendMessage(KafkaProducer<String, String> producer, ProducerRecord<String, String> producerRecord, Map<String, String> pMessage, boolean sync) {
        if (!sync) { // async
            producer.send(producerRecord, (metadata, e) -> {
                if (e == null) {
                    logger.info("async message = {} offset = {}, partition = {}", pMessage.get("key"), metadata.offset(), metadata.partition());
                } else {
                    logger.error("exception error from broker: {}", e.getMessage());
                }
            });
        } else {
            try {
                // 만약 acks=0에 sync로 메시지를 send하면 metadata는 가져옴.
                // partition은 sendbroker안에서 메시지가 저장되지만,
                // offset은 브로커로부터 저장이 됐을때 ack받는 건데 그렇게 하지 못하고 계속 syn보내니 -1 값이 출력됨.
                RecordMetadata recordMetadata = producer.send(producerRecord).get();
                logger.info("sync message = {} offset = {}, partition = {}", pMessage.get("key"), recordMetadata.offset(), recordMetadata.partition());
            } catch (InterruptedException e) { // thread
                logger.error("InterruptedException: {}", e.getMessage());
            } catch (ExecutionException e) { // concurrency
                logger.error("ExecutionException: {}", e.getMessage());
            }
        }

    }
}