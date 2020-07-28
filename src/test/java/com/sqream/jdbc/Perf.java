package com.sqream.jdbc;

import de.vandermeer.asciitable.AsciiTable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.sqream.jdbc.Perf.ColType.*;
import static com.sqream.jdbc.TestEnvironment.createConnection;

public class Perf {
    private static final int[] columnCounts = new int[]{1, 10, 100};
    private static final int[] varcharSizes = new int[]{10, 100, 400};
    private static final int[] rowCounts = new int[]{1, 1000, 10000, 100000, 1000000};
    private static final Map<ColType, BiConsumer<ResultSet, Integer>> gettersMap = new HashMap<>();
    private static int index = 0;
    private static AsciiTable resultTable = new AsciiTable();

    enum ColType {
        BOOL("bool"),
        TINYINT("tinyint"),
        SMALLINT("smallint"),
        INT("int"),
        BIGINT("bigint"),
        REAL("real"),
        DOUBLE("double"),
        DATE("date"),
        DATETIME("datetime"),
        VARCHAR("varchar"),
        NVARCHAR("nvarchar"),
        TEXT("text");

        private final String value;

        ColType(String value) {
            this.value = value;
        }

        public String getValue() {
            return this.value;
        }
    }

    {
        gettersMap.put(BOOL, this::getBool);
        gettersMap.put(TINYINT, this::getTinyInt);
        gettersMap.put(SMALLINT, this::getSmallInt);
        gettersMap.put(INT, this::getInt);
        gettersMap.put(BIGINT, this::getBigInt);
        gettersMap.put(REAL, this::getReal);
        gettersMap.put(DOUBLE, this::getDouble);
        gettersMap.put(DATE, this::getDate);
        gettersMap.put(DATETIME, this::getDatetime);
        gettersMap.put(VARCHAR, this::getText);
        gettersMap.put(NVARCHAR, this::getText);
        gettersMap.put(TEXT, this::getText);
    }

    @Test
    public void selectTest() {
        resultTable.addRow("index", "field", "row length", "columns", "rows", "total ms", "per 1M bytes");
        resultTable.addRule();
        Arrays.stream(values()).forEach(colType -> select(colType));
        System.out.println(resultTable.render());
    }

    private void select(ColType type) {
        BiConsumer<ResultSet, Integer> getter = gettersMap.get(type);
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            for (int colAmount : columnCounts) {
                for (int rowAmount : rowCounts) {
                    for (int rowLength : varcharSizes) {
                        if (isTextType(type) || rowLength == varcharSizes[0]) {
                            long startTime = System.currentTimeMillis();
                            stmt.setFetchSize(1);
                            ResultSet rs = stmt.executeQuery(generateSelectQuery(type, colAmount, rowAmount, rowLength));
                            int rowCounter = 0;
                            while (rs.next()) {
                                for (int i = 0; i < colAmount; i++) {
                                    getter.accept(rs, i);
                                }
                                rowCounter++;
                            }
                            long stopTime = System.currentTimeMillis();
                            resultTable.addRow(index, type, rowLength, colAmount, rowAmount, stopTime - startTime, 0);
                            Assert.assertEquals(rowAmount, rowCounter);
                            index++;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateSelectQuery(ColType type, int colAmount, int rowAmount, int rowLength) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (int i = 0; i < colAmount; i++) {
            sb.append(type.getValue());
            sb.append("?name=col");
            sb.append(i + 1);
            if (isTextType(type)) {
                sb.append("&length=");
                sb.append(rowLength);
            }
            if (i < colAmount - 1) {
                sb.append(",");
            }
        }
        sb.append(" from random limit ");
        sb.append(rowAmount);
        sb.append(";");
        return sb.toString();
    }

    private void getBool(ResultSet rs, int colIndex) {
        try {
            rs.getBoolean(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getTinyInt(ResultSet rs, int colIndex) {
        try {
            rs.getByte(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getSmallInt(ResultSet rs, int colIndex) {
        try {
            rs.getShort(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getInt(ResultSet rs, int colIndex) {
        try {
            rs.getInt(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getBigInt(ResultSet rs, int colIndex) {
        try {
            rs.getLong(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getReal(ResultSet rs, int colIndex) {
        try {
            rs.getFloat(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getDouble(ResultSet rs, int colIndex) {
        try {
            rs.getDouble(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getDate(ResultSet rs, int colIndex) {
        try {
            rs.getDate(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getDatetime(ResultSet rs, int colIndex) {
        try {
            rs.getTimestamp(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getText(ResultSet rs, int colIndex) {
        try {
            rs.getString(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isTextType(ColType type) {
        return type.equals(VARCHAR) || type.equals(NVARCHAR) || type.equals(TEXT);
    }
}


