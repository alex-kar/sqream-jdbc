package com.sqream.jdbc.connector;

import java.sql.SQLOutput;
import java.text.MessageFormat;

public class InsertValidator {

    private ColumnsMetadata metadata;

    public InsertValidator(ColumnsMetadata metadata) {
        this.metadata = metadata;
    }

    public void validateSet(int index, Object value, String type) {
        validateColumnIndex(index);
        validateNullable(index, value);
        validateType(index, type);
    }

    private void validateColumnIndex(int index) {
        if (index <0 || index >= metadata.getRowLength()) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Illegal index [{0}] on get/set\nAllowed indices are [0-{1}]",
                    index, (metadata.getRowLength() - 1)));
        }
    }

    private void validateNullable(int index, Object value) {
        if (value == null && !metadata.isNullable(index)) {
            throw new IllegalArgumentException(
                    MessageFormat.format("Trying to set null on non nullable column number [{0}] of type [{1}]",
                            index + 1, metadata.getType(index)));
        }
    }

    private void validateType(int index, String type) {
        if (!metadata.getType(index).equals(type))
            throw new IllegalArgumentException(
                    MessageFormat.format("Trying to set [{0}] on a column number [{1}] of type [{2}]",
                            type, index + 1, metadata.getType(index)));

    }

    public void validateUbyte(Byte value) {
        if (value != null && value < 0) {
            throw new IllegalArgumentException("Trying to set a negative byte value on an unsigned byte column");
        }
    }
}