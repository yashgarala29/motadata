package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private static final Logger logger = LogManager.getLogger(Consumer.class);
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final String consumerId;

    public Consumer(String consumerId) {
        this.consumerId = consumerId;
        logger.info("Initializing Consumer-{}...", consumerId);

        String bootStrapServer = "localhost:9092";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerId);

        kafkaConsumer = new KafkaConsumer<>(properties);
        logger.info("Consumer-{} initialized successfully.", consumerId);
    }

    public void consume(String topic) {
        try {
            logger.info("Consumer-{} subscribing to topic: {}", consumerId, topic);
            kafkaConsumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Consumer-{} | Partition: {} | Offset: {} | Key: {} | Value: {}",
                            consumerId, record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception ex) {
            logger.error("Error in Consumer-{}: {}", consumerId, ex.getMessage(), ex);
        } finally {
            logger.warn("Shutting down Consumer-{}...", consumerId);
            kafkaConsumer.close();
            logger.info("Consumer-{} has stopped.", consumerId);
        }
    }
}
