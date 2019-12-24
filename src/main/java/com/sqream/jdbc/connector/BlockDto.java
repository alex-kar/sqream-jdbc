package com.sqream.jdbc.connector;

import java.text.MessageFormat;

public class BlockDto {

    private Object[][] columns;
    /**
     * Null values represented as 1 in array.
     */
    private byte[] nullValues;

    public BlockDto(byte[] nullColumns, Object[][] columns) {
        this.nullValues = nullColumns;
        this.columns = columns;
    }

    public int getSize() {
        return columns[0].length;
    }

    public int getRowLength() {
        return columns.length;
    }

    public Object[] getColumn(int index) {
        return columns[index];
    }

    @Override
    public String toString() {
        return MessageFormat.format("Block: [rowLength: {0}, size: {1}]", getRowLength(), getSize());
    }
}
