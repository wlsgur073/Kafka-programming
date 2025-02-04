package com.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class BaseConsumer<K extends Serializable, V extends Serializable> {

    private static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class);

    private KafkaConsumer<K, V> kafkaConsumer;
    private List<String> topics;

    public BaseConsumer(Properties props, List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<>(props); // generic type
        this.topics = topics;
    }

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // VM address
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        BaseConsumer<String, String> baseConsumer = new BaseConsumer<>(props, List.of(topicName));
        String commitMode = "async";
        baseConsumer.init();

        baseConsumer.pollConsumes(1000, commitMode);
        baseConsumer.close();
    }

    private void init() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void close() {
        this.kafkaConsumer.close();
    }

    private void processRecord(ConsumerRecord<K, V> record) {
        logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    private void processRecords(ConsumerRecords<K, V> records) {
        records.forEach(this::processRecord);
    }


    /**
     * shutdown hook을 등록하는 메서드
     * @param kafkaConsumer : kafkaConsumer 객체를 인자로 받아서 shutdown hook을 등록
     */
    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) { // consumer 맴버 변수를 읽기 위해서 인자 전달
        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도의 thread로 kafkaConsumer.wakeup() 호출
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook");
            kafkaConsumer.wakeup();

            try {
                mainThread.join(); // main thread가 종료될 때까지 대기
            } catch (InterruptedException e) {
                logger.error("shutdownHookToRuntime.InterruptedException occurred, {}", e.getMessage());
            }
        }));
    }


    /**
     * pollConsumes() 메서드는 pollCommitSync() 또는 pollCommitAsync() 메서드를 호출
     * @param durationMillis : poll() 메서드의 timeout
     * @param commitMode : commit 방식 (sync, async)
     */
    private void pollConsumes(long durationMillis, String commitMode) {
        try {
            while (true) {
                if (commitMode.equals("sync")) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        } catch (WakeupException e) {
            logger.error("pollConsumes.WakeupException occurred, {}", e.getMessage());
        } catch (Exception e) {
            logger.error("pollConsumes.Exception occurred, {}", e.getMessage());
        } finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync(); // 마지막 poll에서 처리되지 않은 offset을 commit
            logger.info("finally consumer is closing");
            this.close();
        }
    }

    private void pollCommitSync(long durationMillis) {
        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        try {
            if(consumerRecords.count() > 0 ) { // record가 있을 때만 commit
                this.kafkaConsumer.commitSync();
                logger.info("commit sync has been called");
            }
        } catch(CommitFailedException e) {
            logger.error(e.getMessage());
        }
    }

    private void pollCommitAsync(long durationMillis) {
        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);

        this.kafkaConsumer.commitAsync( (offsets, exception) -> {
            if(exception != null) {
                logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
            }
        });
    }

}
