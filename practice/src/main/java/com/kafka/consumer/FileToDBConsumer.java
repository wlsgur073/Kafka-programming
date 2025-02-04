package com.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileToDBConsumer<K extends Serializable, V extends Serializable> {
    public static final Logger logger = LoggerFactory.getLogger(FileToDBConsumer.class.getName());

    private KafkaConsumer<K, V> kafkaConsumer;
    private List<String> topics;

    private OrderDBHandler orderDBHandler;


    /**
     * @param orderDBHandler: DB에 데이터를 저장하기 위한 OrderDBHandler 객체
     */
    public FileToDBConsumer(Properties consumerProps, List<String> topics, OrderDBHandler orderDBHandler) {
        this.kafkaConsumer = new KafkaConsumer<>(consumerProps);
        this.topics = topics;
        this.orderDBHandler = orderDBHandler;
    }

    private void init() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void close() {
        this.kafkaConsumer.close();
        this.orderDBHandler.close(); // close DB connection
    }

    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        Thread mainThread = Thread.currentThread();

        //main thread 종료시 별도의 thread로 KafkaConsumer wakeup() 메소드 호출.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info(" main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("shutdownHookToRuntime.InterruptedException occurred, {}", e.getMessage());
            }
        }));
    }

    private void processRecord(ConsumerRecord<K, V> record) {
        OrderDTO orderDTO = makeOrderDTO(record);
        orderDBHandler.insertOrder(orderDTO);
    }

    private void processRecords(ConsumerRecords<K, V> records) {
        List<OrderDTO> orders = makeOrders(records);
        orderDBHandler.insertOrders(orders);
    }

    private OrderDTO makeOrderDTO(ConsumerRecord<K, V> record) {
        String messageValue = (String) record.value();
        logger.info("###### messageValue: {}", messageValue);

        String[] tokens = messageValue.split(",");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"); // timestamp format

//        Before
//        OrderDTO orderDTO = new OrderDTO(tokens[0], tokens[1], tokens[2], tokens[3],
//                tokens[4], tokens[5], LocalDateTime.parse(tokens[6].trim(), formatter));
//        return orderDTO;

        // fixed
        return new OrderDTO(
                tokens[1], tokens[2], tokens[3],
                tokens[4], tokens[5], tokens[6],
                LocalDateTime.parse(tokens[7].trim(), formatter)
        );
    }

    private List<OrderDTO> makeOrders(ConsumerRecords<K, V> records) {
        List<OrderDTO> orders = new ArrayList<>();
        records.forEach(record -> orders.add(makeOrderDTO(record)));
        return orders;
    }

    public void pollConsumes(long durationMillis, String commitMode) {
        if (commitMode.equals("sync")) {
            pollCommitSync(durationMillis);
        } else {
            pollCommitAsync(durationMillis);
        }
    }

    private void pollCommitAsync(long durationMillis) {
        try {
            while (true) {
                ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
                logger.info("consumerRecords count: {}", consumerRecords.count());
                if (consumerRecords.count() > 0) {
                    try {
                        processRecords(consumerRecords);
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                }

//                if(consumerRecords.count() > 0) {
//                    for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
//                        processRecord(consumerRecord); // record 하나씩 처리 // processRecord() 내부 로직이 매번 DB call하게 돼 있음.
//
//                }

                //commitAsync의 OffsetCommitCallback을 lambda 형식으로 변경
                this.kafkaConsumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                    }
                });
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            close();
        }
    }

    private void pollCommitSync(long durationMillis) {
        try {
            while (true) {
                ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
                processRecords(consumerRecords);
                try {
                    if (consumerRecords.count() > 0) {
                        this.kafkaConsumer.commitSync();
                        logger.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            close();
        }
    }


    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        String url = "jdbc:postgresql://192.168.56.101:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        OrderDBHandler orderDBHandler = new OrderDBHandler(url, user, password);

        FileToDBConsumer<String, String> fileToDBConsumer = new FileToDBConsumer<>(props, List.of(topicName), orderDBHandler);
        fileToDBConsumer.init();
        String commitMode = "async";

        fileToDBConsumer.pollConsumes(1000, commitMode);
        fileToDBConsumer.close();
    }

}