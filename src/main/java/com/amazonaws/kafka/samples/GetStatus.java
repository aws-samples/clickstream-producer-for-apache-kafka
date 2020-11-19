package com.amazonaws.kafka.samples;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GetStatus implements Runnable {
    private static final Logger logger = LogManager.getLogger(KafkaClickstreamClient.class);
    private AtomicBoolean cancel = new AtomicBoolean(false);
    @Override
    public void run() {
        while (!(cancel.get())) {
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                logger.info("Status thread interuppted..");
                cancel.set(true);
            }
            logger.info("Execution time so far in milliseconds: {} \n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - KafkaClickstreamClient.startTime));
            logger.info("Total events sent: {} \n", Events.getTotalEvents());
            logger.info("Producer rate per sec: {} \n", (Events.getTotalEvents()/(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - KafkaClickstreamClient.startTime)/1000)));

        }

    }
}
