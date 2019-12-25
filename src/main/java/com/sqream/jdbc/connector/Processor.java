package com.sqream.jdbc.connector;

import java.io.DataOutputStream;
import java.io.OutputStream;

public class Processor {

    private ColumnsMetadata metadata;

    /**
     * Null values represented as 1 in array.
     * Example: 5 Integers: [10, 15, null, 20, null] will be represented as byte[]{0, 0, 1, 0, 1}.
     */
    private byte[][] nullValues;

    private byte[][] nvarcLenColumns;

    public Processor(ColumnsMetadata metadata) {
        this.metadata = metadata;
    }

    public void prepareData(BlockDto block, OutputStream outputStream) {
        DataOutputStream stream = new DataOutputStream(outputStream);
    }

//    setNullableValue(index, value);
//
//    private void setNullableValue(int index, Object value) {
//        if (value == null) {
//            nullValues[index][cursor] = (byte) 1;
//        }
//    }
}
