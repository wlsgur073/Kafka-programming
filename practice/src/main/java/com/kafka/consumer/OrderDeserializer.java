package com.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.model.OrderRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrderDeserializer implements Deserializer<OrderRecord> {

    public static final Logger logger = LoggerFactory.getLogger(OrderDeserializer.class.getName());
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());


    @Override
    public OrderRecord deserialize(String s, byte[] bytes) {
        // Kafka의 메시지는 byte[] 전달.
        OrderRecord orderRecord = null;

        try {
            orderRecord = objectMapper.readValue(bytes, OrderRecord.class);
        } catch (IOException e) {
            logger.error("OrderDeserializer.deserialize.IOException occurred, {}", e.getMessage());
        }

        return orderRecord;
    }
}
