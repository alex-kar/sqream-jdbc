package com.sqream.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.sqream.jdbc.TestEnvironment.createConnection;

public class SQ2972 {

    @Test
    public void twoParallelInsertStatementTest() throws SQLException {

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("create or replace table test_table_1 (col1 int);");
            stmt.executeUpdate("create or replace table test_table_2 (col1 int);");
        }

        try (Connection conn1 = createConnection();
             Connection conn2 = createConnection();
             PreparedStatement pstmt1 = conn1.prepareStatement("insert into test_table_1 values (?);");
             PreparedStatement pstmt2 = conn2.prepareStatement("insert into test_table_2 values (?);")) {

            ExecutorService executorService = Executors.newFixedThreadPool(2);
            executorService.submit(() -> setTestValue(pstmt1));
            executorService.submit(() -> setTestValue(pstmt2));
        }
    }

    private void setTestValue(PreparedStatement pstmt) {
        try {
            pstmt.setInt(1, 1);
            pstmt.addBatch();
            pstmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
