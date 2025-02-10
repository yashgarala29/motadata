package org.example;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Collections;
import java.util.logging.Logger;


@ExtendWith(MockitoExtension.class)
public class ConsumerTest {
    @Mock
    private KafkaConsumer<String, String> kafkaConsumer;

    @Mock
    private Logger logger;

    @InjectMocks
    private Consumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new Consumer("test-consumer");
        consumer = spy(consumer);

    }

    @Test
    void testConsume_SubscribesAndPolls() {
        when(kafkaConsumer.poll(Duration.ofMillis(100))).thenReturn(ConsumerRecords.empty());

        // Run the consume method in a separate thread and interrupt it
        Thread testThread = new Thread(() -> consumer.consume("test-topic"));
        testThread.start();

        // Allow some time for execution
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        testThread.interrupt();

        verify(kafkaConsumer, atLeastOnce()).subscribe(Collections.singletonList("test-topic"));

        verify(kafkaConsumer, atLeastOnce()).poll(Duration.ofMillis(100));

        verify(kafkaConsumer, atLeastOnce()).close();
    }
}
