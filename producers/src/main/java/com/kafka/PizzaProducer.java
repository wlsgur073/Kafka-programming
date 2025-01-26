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

public class PizzaProducer {
    private static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class);


    public static void main(String[] args) {
        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks setting
        // props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // 0, 1, all

        // batch setting
//        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // default 16KB
//        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1"); // default 0 // 설정 값만큼 배치에 메시지가 차길 기다린 후 브로커로 전송
//        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // default 5, range 1-5 // 브로커 서버의 응답없이(without ack) Producer의 sender thread가 한번에 보낼 수 있는 메시지 배치의 개수

        // delivery.timeout.ms: 설정 시간동안 ack가 오지 않으면 에러를 발생시킴.
        // delivery.timeout.ms > linger.ms + request.timeout.ms 이어야 함.
//        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "29000"); // ConfigException: delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms

        // ENABLE_IDEMPOTENCE_CONFIG의 기본값은 true인데, 명시적으로 설정해주면 acks를 all로 설정해줘야 함. 그렇게 안하면 에러 발생.
//        props.setProperty(ProducerConfig.ACKS_CONFIG, "0");
//        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Must set acks to all in order to use the idempotent producer.


        // Create KafkaProducer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // create network thread by KafkaProducer

        sendPizzaMessage(topicName, producer,
                -1, 1000, 0, 0, false
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