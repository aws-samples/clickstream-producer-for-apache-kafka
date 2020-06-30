package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class Events {
    static final private String[] deviceType = {"mobile","computer", "tablet"};
    static final private String[] productCatalogOptions = {"home_page", "product_detail"};
    static final private String[] productTypeOptions = {"cell phones", "laptops", "ear phones", "soundbars", "cd players", "AirPods", "video games", "cameras"};
    static final private String[] productDetailOptions = {"product_catalog", "add_to_cart"};
    static final private String[] addToCartOptions = {"product_catalog", "remove_from_cart", "order"};
    static final private String[] orderOptions = {"order_checkout", "remove_from_cart", "product_catalog"};
    static final private String[] removeFromCartOptions = {"", "product_detail", "product_catalog"};
    private Random rand = new Random();
    private final static AtomicLong totalEvents = new AtomicLong(0L);
    private AtomicInteger errorCount = new AtomicInteger(0);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private final Map<TopicPartition, Long> lastOffset = new HashMap<>();
    private Long previousGlobalSeqNo = 0L;

    private static final Logger logger = LogManager.getLogger(Events.class);

    static Long getTotalEvents(){
        return totalEvents.longValue();
    }

    int getThreadEventCount(){
        return eventCount.intValue();
    }

    int getErrorCount(){
        return errorCount.intValue();
    }

    Map<TopicPartition, Long> getLastOffset(){
        return lastOffset;
    }

    Long getLastGlobalSeqNo(){
        return previousGlobalSeqNo;
    }

    void genEvents(Producer<String, ClickEvent> kafkaProducer, Integer userID) throws Exception {

        ClickEvent event;
        String userDeviceType = deviceType[rand.nextInt(deviceType.length)];
        String userIP = "66.249.1." + rand.nextInt(255);
        String previousEventType = null;
        String previousProductType = null;
        do {

            event = genUserEvent(userID, userDeviceType, previousEventType, previousProductType, userIP, previousGlobalSeqNo);
            previousEventType = event.getEventType().toString();
            previousProductType = event.getProductType().toString();
            previousGlobalSeqNo = event.getGlobalseq();

            String localEventFileLocation = "/tmp/Clickstream.txt";
            if (!KafkaClickstreamClient.nologgingEvents)
                Util.eventWriter(event.toString(), localEventFileLocation, true, "clickstream");

            kafkaProducer.send(new ProducerRecord<>(KafkaClickstreamClient.topic, userID.toString(), event), (recordMetadata, e) -> {
                if (e != null) {
                    logger.error(Util.stackTrace(e));
                    errorCount.getAndIncrement();
                } else {
                    if (recordMetadata.hasOffset()) {
                        lastOffset.put(new TopicPartition(recordMetadata.topic(), recordMetadata.partition()), recordMetadata.offset());
                        eventCount.getAndIncrement();
                        totalEvents.getAndIncrement();
                    }
                }
            });

        } while (!(event.getEventType().toString().equals("")) && errorCount.get() < 1);

    }

    private ClickEvent genUserEvent(Integer userId, String userDeviceType, String previousEventType, String previousProductType, String userIP, Long previousGlobalSeqNo){

        String eventType;
        String productType;
        if (previousEventType == null){
            eventType = "home_page";
            productType = "N/A";
        }
        else {
            eventType = nextEventType(previousEventType);
            productType = nextProductType(previousProductType, eventType);

        }

        return ClickEvent.newBuilder()
                .setIp(userIP)
                .setProductType(productType)
                .setUserid(userId)
                .setEventtimestamp(System.currentTimeMillis())
                .setDevicetype(userDeviceType)
                .setEventType(eventType)
                .setGlobalseq(KafkaClickstreamClient.counter.incrementAndGet())
                .setPrevglobalseq(previousGlobalSeqNo)
                .build();

    }

    private String nextEventType(String previousEventType){
        String eventType = "";

        switch (previousEventType) {
            case "home_page":
                eventType = "product_catalog";
                break;
            case "product_catalog":
                eventType = productCatalogOptions[rand.nextInt(productCatalogOptions.length)];
                break;
            case "product_detail":
                eventType = productDetailOptions[rand.nextInt(productDetailOptions.length)];
                break;
            case "add_to_cart":
                eventType = addToCartOptions[rand.nextInt(addToCartOptions.length)];
                break;
            case "order":
                eventType = orderOptions[rand.nextInt(orderOptions.length)];
                break;
            case "order_checkout":
                eventType = "";
                break;
            case "remove_from_cart":
                eventType = removeFromCartOptions[rand.nextInt(removeFromCartOptions.length)];
                break;
        }
        return eventType;
    }

    private String nextProductType(String previousProductType, String eventType){
        String productType = "";

        switch (eventType) {
            case "home_page":
                productType = "N/A";
                break;
            case "product_catalog":
                productType = "N/A";
                break;
            case "product_detail":
                productType = productTypeOptions[rand.nextInt(productTypeOptions.length)];
                break;
            case "add_to_cart":
                productType = previousProductType;
                break;
            case "remove_from_cart":
                productType = previousProductType;
                break;
            case "order":
                productType = previousProductType;
                break;
            case "order_checkout":
                productType = previousProductType;
                break;
        }
        return productType;
    }
}
