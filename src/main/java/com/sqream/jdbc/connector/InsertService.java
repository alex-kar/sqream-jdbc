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
    private BlockingQueue<BlockDto> queue;
    /**
     * Amount of rows in block
     */
    private int blockSize;
    /**
     * Amount of added rows in current block
     */
    private int rowCounter;
    private InsertValidator validator;

    private long t0 = System.currentTimeMillis();

    public InsertService(ColumnsMetadata metadata, int blockSize) {
        this.metadata = metadata;
        this.blockSize = blockSize;
        this.queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        this.validator = new InsertValidator(metadata);
        resetBuilder();
    }

    public void addValue(int index, Object value, String type) {
        validator.validateSet(index, value, type);
        if (blockBuilder.isFull()) {
            try {
                queue.put(blockBuilder.buildBlock());
            } catch (InterruptedException e) {
                // FIXME: Alex K 25.12.19 Figure out how to deal with InterruptedException.
                e.printStackTrace();
            }
        }
        blockBuilder.addValue(index, value);
    }

    public void nextRow() throws InterruptedException {
        blockBuilder.buildRow();
        rowCounter++;
        if (rowCounter == blockSize) {

            long t1 = System.currentTimeMillis();
            System.out.println(String.format("Block: %s ms", (t1 - t0)));
            t0 = System.currentTimeMillis();

            queue.put(blockBuilder.buildBlock());
            resetBuilder();
            LOGGER.log(Level.FINE,
                    MessageFormat.format("Putted block in queue. Queue size: [{0}]", queue.size()));
        }
    }

    public BlockingQueue<BlockDto> getQueue() {
        return queue;
    }

    private void resetBuilder() {
        blockBuilder = new BlockBuilder(metadata.getRowLength(), blockSize, metadata);
        rowCounter = 0;
    }
}
