package com.sqream.jdbc.connector;

import java.io.DataOutputStream;
import java.io.OutputStream;

public class Processor {

    private ColumnsMetadata metadata;

    public Processor(ColumnsMetadata metadata) {
        this.metadata = metadata;
    }

    public void prepareData(BlockDto block, OutputStream outputStream) {
        DataOutputStream stream = new DataOutputStream(outputStream);
    }
}
