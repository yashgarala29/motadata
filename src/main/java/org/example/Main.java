package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Starting Kafka Producer and Consumer...");
        initProducer();
        initConsumer();
    }

    private static void initProducer() {
        for (int i = 0; i < 4; i++) {
            Producer producer = new Producer();
            logger.info("Initializing Producer-{}", i);
            sendMultipleMessages(producer, i);
        }
    }

    private static void sendMultipleMessages(Producer producer, int id) {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger failCounter = new AtomicInteger(0);

        logger.info("Producer-{} started sending messages...", id);

        for (int i = 0; i < 10; i++) {
            String message = "Message-" + id + "-" + counter.getAndIncrement();
            boolean status = producer.send("my_first", message);

            if (status) {
                successCounter.incrementAndGet();
            } else {
                failCounter.incrementAndGet();
            }
            if(i==8){

                producer.flush();
                producer.close();
            }
        }


        logger.info("Producer-{} completed. Success: {}, Fail: {}", id, successCounter.get(), failCounter.get());
    }

    private static void initConsumer() {
        Consumer consumer1 = new Consumer("1");
        logger.info("Initializing Consumer-1...");
        Thread thread1 = new Thread(() -> consumer1.consume("my_first"));
        thread1.start();
    }
}
