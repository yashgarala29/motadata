package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Producer {
    private static final Logger logger = LogManager.getLogger(Producer.class);
    private final Properties properties;
    private final KafkaProducer<String, String> kafkaProducer;

    public Producer() {
        String bootStrapServer = "localhost:9092";
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        kafkaProducer = new KafkaProducer<>(properties);
        logger.info("Kafka Producer initialized successfully.");
    }

    public boolean send(String topic, String message) {
        CountDownLatch latch = new CountDownLatch(1);
        final boolean[] isSuccess = {false};

        logger.info("Sending message to topic: [{}], message: [{}]", topic, message);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        try {
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Message sent successfully to topic: [{}], partition: [{}], offset: [{}]",
                            metadata.topic(), metadata.partition(), metadata.offset());
                    isSuccess[0] = true;
                } else {
                    logger.error("Message send failed to topic: [{}], message: [{}]", topic, message, exception);
                    isSuccess[0] = false;
                }
                latch.countDown();
            });

            latch.await(); // Wait for the callback to complete
        } catch (Exception ex) {
//            logger.error("Unexpected error while sending message to topic: [{}], message: [{}]", topic, message, ex);
            return false;
        }
        return isSuccess[0];
    }

    public void flush() {
        logger.info("Flushing Kafka Producer...");
        kafkaProducer.flush();
    }

    public void close() {
        logger.info("Closing Kafka Producer...");
        kafkaProducer.close();
    }
}
