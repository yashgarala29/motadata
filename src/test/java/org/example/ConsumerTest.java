package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.logging.Logger;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ConsumerTest {

    @Mock
    private KafkaConsumer<String, String> kafkaConsumer;  // Mocked Kafka Consumer

    @Mock
    private Logger logger;  // Mocked Logger

    @InjectMocks
    private Consumer consumer;  // Consumer instance with injected mocks

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this); // Ensure mocks are initialized
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

        // Verify Kafka consumer behavior
        verify(kafkaConsumer, atLeastOnce()).subscribe(Collections.singletonList("test-topic"));
        verify(kafkaConsumer, atLeastOnce()).poll(Duration.ofMillis(100));
        verify(kafkaConsumer, atLeastOnce()).close();
    }
}
