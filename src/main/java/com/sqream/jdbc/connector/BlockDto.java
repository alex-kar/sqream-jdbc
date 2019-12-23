package com.sqream.jdbc.connector;

public class BlockDto {

    private Object[][] columns;

    public BlockDto(Object[][] columns) {
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
}
