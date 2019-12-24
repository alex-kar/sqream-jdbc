package com.sqream.jdbc.connector;

import java.text.MessageFormat;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InsertService {
    private static final Logger LOGGER = Logger.getLogger(InsertService.class.getName());

    private static final int QUEUE_SIZE = 10;

    private BlockBuilder blockBuilder;
    private ColumnsMetadata metadata;
    private BlockingQueue queue;
    /**
     * Amount of rows in block
     */
    private int blockSize;
    /**
     * Amount of added rows in current block
     */
    private int rowCounter;

    public InsertService(ColumnsMetadata metadata, int blockSize) {
        this.metadata = metadata;
        this.blockSize = blockSize;
        this.queue = new ArrayBlockingQueue(QUEUE_SIZE);
        resetBuilder();
    }

    public void resetBuilder() {
        blockBuilder = new BlockBuilder(metadata.getRowLength(), blockSize, metadata);
        rowCounter = 0;
    }

    public void addValue(int index, Object value) throws ConnException {
        blockBuilder.addValue(index, value);
    }

    public void nextRow() throws ConnException, InterruptedException {
        blockBuilder.buildRow();
        rowCounter++;
        if (rowCounter == blockSize) {
            queue.put(blockBuilder.buildBlock());
            resetBuilder();
            LOGGER.log(Level.FINE, MessageFormat.format("Put block in queue. Queue size: [{0}]", queue.size()));
        }
    }
}
