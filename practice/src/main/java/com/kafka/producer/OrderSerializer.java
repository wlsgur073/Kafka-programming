package com.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.model.OrderRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderSerializer implements Serializer<OrderRecord> {

    public static final Logger logger = LoggerFactory.getLogger(OrderSerializer.class.getName());

    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()); // for LocalDateTime

    @Override
    public byte[] serialize(String s, OrderRecord orderRecord) {
        byte[] retVal = null;

        try {
            retVal = objectMapper.writeValueAsBytes(orderRecord);
        } catch (JsonProcessingException e) {
            logger.error("OrderSerializer.serialize.JsonProcessingException occurred, {}", e.getMessage());
        }
        return retVal;
    }
}
