package com.kafka.consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Objects;

public class JDBCTester {

    @Test
    @DisplayName("PostgreSQL 연결 테스트")
    void test() {

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;


        String url = "jdbc:postgresql://192.168.56.101:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        try {
            conn = DriverManager.getConnection(url, user, password);
            st = conn.createStatement();
            rs = st.executeQuery("SELECT 'postgresql is connected' ");

            if (rs.next())
                System.out.println(rs.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                Objects.requireNonNull(rs).close();
                st.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}