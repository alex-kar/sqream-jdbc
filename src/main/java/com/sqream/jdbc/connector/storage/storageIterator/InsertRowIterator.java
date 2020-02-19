package com.sqream.jdbc.connector.storage.storageIterator;

import java.text.MessageFormat;

public class InsertRowIterator implements RowIterator {
    private static final int INIT_ROW_INDEX = 0;
    private int rowIndex = INIT_ROW_INDEX;
    private int rowAmount;

    public InsertRowIterator(int rowAmount) {
        this.rowAmount = rowAmount;
    }

    @Override
    public boolean next() {
        return ++rowIndex < rowAmount;
    }

    @Override
    public void reset() {
        rowIndex = INIT_ROW_INDEX;
    }

    @Override
    public int getRowIndex() {
        if (rowIndex >= rowAmount) {
            throw new IndexOutOfBoundsException(MessageFormat.format(
                    "Call next() before getRowIndex() or reset iterator. Current rowIndex=[{0}], rowAmount=[{1}]",
                    rowIndex, rowAmount));
        }
        return rowIndex;
    }
}