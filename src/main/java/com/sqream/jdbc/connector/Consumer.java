package com.sqream.jdbc.connector;

import java.text.MessageFormat;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Consumer {
    private static final Logger LOGGER = Logger.getLogger(Consumer.class.getName());

    private ExecutorService executorService;
    private Processor processor;

    public Consumer(int nThreads, Processor processor) {
        this.executorService = Executors.newFixedThreadPool(nThreads);
        this.processor = processor;
    }

    public void start(BlockingQueue<BlockDto> queue) {
        executorService.execute(() -> {
            try {
                while (true) {
                    BlockDto block = queue.take();
                    LOGGER.log(Level.FINE, MessageFormat.format("Start processing block[{0}]. Queue remaining: [{1}]", block, queue.size()));
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
