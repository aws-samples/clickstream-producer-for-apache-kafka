package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.producer.*;
import samples.clickstream.avro.ClickEvent;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.lang.InterruptedException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaClickstreamClient {

    private static final Logger logger = LogManager.getLogger(KafkaClickstreamClient.class);
    static AtomicLong counter = new AtomicLong(0);
    static Long startTime;

    @Parameter(names = {"--numberOfUsers", "-nou"})
    static Integer numberOfUsers = Integer.MAX_VALUE;
    static AtomicBoolean cancel = new AtomicBoolean(false);

    @Parameter(names = {"--help", "-h"}, help = true)
    private boolean help = false;

    @Parameter(names = {"--topic", "-t"})
    static String topic = "ExampleTopic";

    @Parameter(names = {"--propertiesFilePath", "-pfp"})
    private static String propertiesFilePath = "/tmp/kafka/producer.properties";

    @Parameter(names = {"--numThreads", "-nt"})
    static Integer numThreads = 2;

    @Parameter(names = {"--runFor", "-rf"})
    private static Integer runFor = 0;

    @Parameter(names = {"--noDelay", "-nd"})
    static boolean noDelay = false;

    @Parameter(names = {"--sslEnable", "-ssl"})
    static boolean sslEnable = false;

    @Parameter(names = {"--mTLSEnable", "-mtls"})
    static boolean mTLSEnable = false;

    @Parameter(names = {"--glueSchemaRegistry", "-gsr"})
    static boolean glueSchemaRegistry = false;

    @Parameter(names = {"--gsrRegistryName", "-grn"})
    static String gsrRegistryName;

    @Parameter(names = {"--gsrSchemaName", "-gsn"})
    static String gsrSchemaName;

    @Parameter(names = {"--gsrSchemaDescription", "-gsd"})
    static String gsrSchemaDescription;

    @Parameter(names = {"--saslscramEnable", "-sse"})
    static boolean saslscramEnable = false;

    @Parameter(names = {"--saslscramUser", "-ssu"})
    static String saslscramUser;

    @Parameter(names = {"--region", "-reg"})
    static String region = "us-east-1";

    @Parameter(names = {"--gsrRegion", "-gsrr"})
    static String gsrRegion = "us-east-1";

    @Parameter(names = {"--gsrAutoRegistration", "-gar"})
    static boolean gsrAutoRegistration = false;

    @Parameter(names = {"--gsrCompatibilitySetting", "-gcs"})
    static String gsrCompatibilitySetting;

    @Parameter(names = {"--nologgingEvents", "-nle"})
    static boolean nologgingEvents = false;

    static long startingGlobalSeqNo = 0;

    private static <T> Collection<Future<T>> submitAll(ExecutorService service, Collection<? extends Callable<T>> tasks) {
        Collection<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(service.submit(task));
        }
        return futures;
    }

    private void shutdown(List<RunProducer> executeTasks, ExecutorService executor, Producer<String, ClickEvent> kafkaProducer) {
        logger.info("Starting exit..");

        logger.info("Shutting down producer(s)...");
        for (RunProducer producer : executeTasks) {
            producer.shutdown();
        }

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            logger.error(Util.stackTrace(e));
        }
        try {
            executor.shutdownNow();
            while (!executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                logger.info("Not yet. Still waiting for producer(s) termination");
            }
        } catch (InterruptedException e) {
            logger.error(Util.stackTrace(e));
        }

        logger.info("Flushing producer..");
        kafkaProducer.flush();
        logger.info("Closing producer..");
        kafkaProducer.close();
        long endTime = System.nanoTime();
        logger.info("End Timestamp {}\n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
        long executionTime = endTime - startTime;
        logger.info("Execution time in milliseconds: {} \n", TimeUnit.NANOSECONDS.toMillis(executionTime));
        logger.info("Total events sent: {} \n", Events.getTotalEvents());
        logger.info("Last Global seq no: {} \n", counter.get());


    }

    private void runProducer() throws Exception {

        startTime = System.nanoTime();
        logger.info("Start time: {} \n", TimeUnit.NANOSECONDS.toMillis(startTime));

        ExecutorService executor = Executors.newFixedThreadPool(numThreads + 1);
        List<RunProducer> executeTasks = new ArrayList<>();

        final Producer<String, ClickEvent> kafkaProducer = new KafkaProducerFactory(propertiesFilePath, sslEnable, mTLSEnable, saslscramEnable, glueSchemaRegistry).createProducer();

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(executeTasks, executor, kafkaProducer)));

        for (Integer i = 0; i < numThreads; i++) {
            executeTasks.add(new RunProducer(kafkaProducer));
        }

        Collection<Future<String>> futures = submitAll(executor, executeTasks);
        String result;

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Future<String> future : futures) {
            try {
                logger.trace("Future status: {} ", future.isDone());
                if (future.isDone()) {
                    result = future.get();
                    logger.info(result);
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.error(Util.stackTrace(e));
                System.exit(1);
            }
        }

        executor.submit(new GetStatus());


        int tasksDone = 0;
        if (runFor > 0) {
            int runTime = 0;

            while (runTime < runFor) {
                try {
                    TimeUnit.SECONDS.sleep(2L);
                } catch (InterruptedException e) {
                    logger.error(Util.stackTrace(e));
                }
                for (Future<String> future : futures) {
                    if (future.isDone()) {
                        tasksDone += 1;
                        if (tasksDone == numThreads) {
                            System.exit(0);
                        }

                    }
                }
                runTime += 2;
            }
            logger.info("Reached specified run time of {} seconds. Shutting down. \n", runFor);
            System.exit(0);
        }

        while (tasksDone < numThreads) {
            try {
                TimeUnit.SECONDS.sleep(2L);
            } catch (InterruptedException e) {
                logger.error(Util.stackTrace(e));
            }
            for (Future<String> future : futures) {
                if (future.isDone()) {
                    tasksDone += 1;
                }
            }
        }

        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        KafkaClickstreamClient client = new KafkaClickstreamClient();
        JCommander jc = JCommander.newBuilder()
                .addObject(client)
                .build();
        jc.parse(args);
        if (client.help) {
            jc.usage();
            return;
        }
        ParametersValidator.validate();
        client.runProducer();
    }

}
