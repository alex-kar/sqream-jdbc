package com.sqream.jdbc;

import de.vandermeer.asciitable.AsciiTable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
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
    private static final int selectAllColAmount = 200;
    private static final int varcharSizeForSelectAll = 10;
    private static final int limitForSelectAll = 100000;
    private static final Map<ColType, BiConsumer<ResultSet, Integer>> gettersMap = new HashMap<>();
    private static int index = 0;
    private static AsciiTable resultTable = new AsciiTable();

    enum ColType {
        BOOL("bool", Byte.BYTES),
        TINYINT("tinyint", Byte.BYTES),
        SMALLINT("smallint", Short.BYTES),
        INT("int", Integer.BYTES),
        BIGINT("bigint", Long.BYTES),
        REAL("real", Float.BYTES),
        DOUBLE("double", Double.BYTES),
        DATE("date", Integer.BYTES),
        DATETIME("datetime", Long.BYTES),
        VARCHAR("varchar", Byte.BYTES),
        NVARCHAR("nvarchar", Byte.BYTES);

        private final String value;
        private final int size;

        ColType(String value, int size) {
            this.value = value;
            this.size = size;
        }

        public String getValue() {
            return this.value;
        }

        public int getSize() {
            return this.size;
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
    }

    @Test
    public void selectTest() {
        resultTable.addRule();
        resultTable.addRow("index", "field", "row length", "columns", "rows", "total ms", "per 1M bytes");
        resultTable.addRule();
        Arrays.stream(values()).forEach(this::select);
        resultTable.addRule();
        System.out.println(resultTable.render());
    }

    private void select(ColType type) {
        BiConsumer<ResultSet, Integer> getter = gettersMap.get(type);
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            for (int colAmount : columnCounts) {
                for (int rowAmount : rowCounts) {
                    for (int textLength : varcharSizes) {
                        if (isTextType(type) || textLength == varcharSizes[0]) {
                            long startTime = System.currentTimeMillis();
                            stmt.setFetchSize(1);
                            ResultSet rs = stmt.executeQuery(generateSelectQuery(type, colAmount, rowAmount, textLength));
                            int rowCounter = 0;
                            while (rs.next()) {
                                for (int i = 0; i < colAmount; i++) {
                                    getter.accept(rs, i);
                                }
                                rowCounter++;
                            }
                            long totalTime = System.currentTimeMillis() - startTime;
                            long rowLength = rowLength(type, colAmount, textLength);
                            resultTable.addRow(index, type, rowLength, colAmount, rowAmount, totalTime, (1024 * 1024 * totalTime) / (rowLength * rowAmount));
                            Assert.assertEquals(rowAmount, rowCounter);
                            index++;
                        }
                    }
                }
            }
            int colAmountPerType = selectAllColAmount / ColType.values().length;
            long startTime = System.currentTimeMillis();
            selectAll(conn, colAmountPerType);
            long totalTime = System.currentTimeMillis() - startTime;
            long rowLength = rowLengthAll(colAmountPerType);
            resultTable.addRow(index, "ALL", rowLength, ColType.values().length * colAmountPerType, limitForSelectAll, totalTime, (1024 * 1024 * totalTime) / (rowLength * limitForSelectAll));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void selectAll(Connection conn, int colAmountPerType) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(generateSelectAllQuery(colAmountPerType));
            BiConsumer<ResultSet, Integer> getter;
            int colIndex;
            while (rs.next()) {
                colIndex = 0;
                for (ColType type : ColType.values()) {
                    getter = gettersMap.get(type);
                    for (int i = 0; i < colAmountPerType; i++) {
                        getter.accept(rs, colIndex);
                        colIndex++;
                    }
                }
            }
        }
    }

    private String generateSelectQuery(ColType type, int colAmount, int rowAmount, int textLength) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (int i = 0; i < colAmount; i++) {
            sb.append(generateColDefinition(type, textLength, i));
            if (i < colAmount - 1) {
                sb.append(", ");
            }
        }
        sb.append(" from random limit ");
        sb.append(rowAmount);
        sb.append(";");
        return sb.toString();
    }

    private String generateColDefinition(ColType colType, int textLength, int colIndex) {
        StringBuilder sb = new StringBuilder();
        sb.append(colType.getValue());
        sb.append("?name=col");
        sb.append(colIndex + 1);
        if (isTextType(colType)) {
            sb.append("&length=");
            sb.append(textLength);
        }
        return sb.toString();
    }

    private String generateSelectAllQuery(int colAmountPerType) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (ColType type : ColType.values()) {
            for (int i = 0; i < colAmountPerType; i++) {
                sb.append(generateColDefinition(type, varcharSizeForSelectAll, i));
                if (i < colAmountPerType - 1) {
                    sb.append(", ");
                }
            }
            sb.append(", ");
        }
        sb.append(" from random limit ");
        sb.append(limitForSelectAll);
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
        return type.equals(VARCHAR) || type.equals(NVARCHAR);
    }

    private long rowLength(ColType type, int colAmount, int textLength) {
        return isTextType(type) ?
                textLength * Byte.BYTES * colAmount :
                type.getSize() * colAmount;
    }

    private long rowLengthAll(int colAmountPerType) {
        long result = 0;
        for (ColType colType: ColType.values()) {
            for (int i = 0; i < colAmountPerType; i++) {
                result += isTextType(colType) ?
                        varcharSizeForSelectAll * Byte.BYTES :
                        colType.getSize();
            }
        }
        return result;
    }
}
