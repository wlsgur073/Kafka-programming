package com.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;

public class TestMain {

    @Test
    @DisplayName("Show inetAddress.getHostAddress()")
    void showInetAddress() {
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getLocalHost();

            System.out.println("inetAddress = " + inetAddress.getHostAddress());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
