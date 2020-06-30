package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RunProducer implements Callable<String>{

    private static final Logger logger = LogManager.getLogger(RunProducer.class);
    private final Producer<java.lang.String, ClickEvent> kafkaProducer;
    private boolean cancel = false;
    private static AtomicInteger userIDMax = new AtomicInteger(1000);
    private Random rand = new Random();

    RunProducer(Producer<java.lang.String, ClickEvent> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    void shutdown() {
        cancel = true;
    }

    private void runProducer() throws Exception {
        Integer userCount = 0;
        Integer currUserIDLimit;
        Integer userIDLimit;
        Events producer = new Events();

        logger.info("Starting producer in thread {} \n", Thread.currentThread().getName());

        while (userCount < KafkaClickstreamClient.numberOfUsers) {
            String localBookmarkFileLocation = "/tmp/bookmark.txt";
            if (!cancel) {

                currUserIDLimit = userIDMax.get();
                userIDLimit = userIDMax.addAndGet(1000);
                Integer userID = rand.nextInt((userIDLimit - currUserIDLimit)) + currUserIDLimit;

                if (KafkaClickstreamClient.counter.get() == 0L) {
                    Util.setGlobalSeqNo(localBookmarkFileLocation);
                }
                producer.genEvents(kafkaProducer, userID);

            } else {
                logger.info("Cancel is true. Shutting down thread: {} \n", Thread.currentThread().getName());
                if (producer.getLastOffset() != null){
                    if (!producer.getLastOffset().isEmpty()){
                        logger.info("{} - Last Offset: {} \n", Thread.currentThread().getName(), producer.getLastOffset());
                    }

                    if (producer.getLastGlobalSeqNo() != null) {
                        logger.info("{} - Last Global Seq No: {} \n", Thread.currentThread().getName(), producer.getLastGlobalSeqNo());
                        Util.eventWriter(producer.getLastGlobalSeqNo().toString(), localBookmarkFileLocation, false, "bookmark");
                    }
                }
                break;
            }
            if (producer.getErrorCount() > 0) {
                Util.eventWriter(producer.getLastGlobalSeqNo().toString(), localBookmarkFileLocation, false, "bookmark");
                break;
            }
            userCount += 1;
            if (!(KafkaClickstreamClient.noDelay)) {
                Util.Sleep(500);
            }
        }

        logger.info("User Count in {}: {} \n", Thread.currentThread().getName(), userCount);
        logger.info("Event Count in {}: {} \n", Thread.currentThread().getName(), producer.getThreadEventCount());
        logger.info("{} - Exiting \n", Thread.currentThread().getName());
    }

    @Override
    public String call() throws Exception {
        runProducer();
        return "Task executed in " + Thread.currentThread().getName();
    }
}
