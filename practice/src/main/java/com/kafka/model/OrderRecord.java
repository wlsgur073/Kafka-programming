package com.kafka.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public record OrderRecord(
        String orderId
        , String shopId
        , String menuName
        , String userName
        , String phoneNumber
        , String address
        , LocalDateTime orderTime
        ) implements Serializable {
}
