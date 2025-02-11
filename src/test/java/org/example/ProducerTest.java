package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProducerTest {

    @InjectMocks
    private Producer producer;

    @Mock
    private KafkaProducer<String, String> kafkaProducerMock;

    @BeforeEach
    void setUp() {
        // Inject mock KafkaProducer into the Producer instance
        producer = new Producer();
    }

    @Test
    void testSendMessage_Success() throws ExecutionException, InterruptedException {
        // Mock Kafka send success response
        Future<RecordMetadata> futureMock = mock(Future.class);
        when(futureMock.get()).thenReturn(mock(RecordMetadata.class));
        doAnswer(invocation -> {
            ProducerRecord<String, String> record = invocation.getArgument(0);
            ((org.apache.kafka.clients.producer.Callback) invocation.getArgument(1))
                    .onCompletion(mock(RecordMetadata.class), null);
            return null;
        }).when(kafkaProducerMock).send(any(), any());

        // Call the send method
        boolean result = producer.send("test-topic", "Hello Kafka");

        // Verify that the message was sent
        assertTrue(result);
        verify(kafkaProducerMock, times(1)).send(any(), any());
    }

    @Test
    void testSendMessage_Failure() {
        // Simulate an exception while sending message
        doAnswer(invocation -> {
            ((org.apache.kafka.clients.producer.Callback) invocation.getArgument(1))
                    .onCompletion(null, new RuntimeException("Kafka error"));
            return null;
        }).when(kafkaProducerMock).send(any(), any());

        // Call the send method
        boolean result = producer.send("test-topic", "Hello Kafka");

        // Verify failure case
        assertFalse(result);
        verify(kafkaProducerMock, times(1)).send(any(), any());
    }

    @Test
    void testFlush() {
        // Call flush method
        producer.flush();

        // Verify flush is called on KafkaProducer
        verify(kafkaProducerMock, times(1)).flush();
    }

    @Test
    void testClose() {
        // Call close method
        producer.close();

        // Verify close is called on KafkaProducer
        verify(kafkaProducerMock, times(1)).close();
    }
}
