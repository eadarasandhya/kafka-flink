package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;   // Kafka producer client
import org.apache.kafka.clients.producer.ProducerRecord;  // Represents a message to send
import java.util.Properties;  // For Kafka configuration


//not required only testing purpose
/**
 * TestKafka
 *
 * A simple Kafka producer example that sends 1 million messages
 * to the topic "qwerty". Demonstrates basic producer configuration,
 * high-throughput tuning, and message publishing.
 */
public class TestKafka {

    /**
     * Main method that initializes Kafka producer properties,
     * creates a producer instance, sends messages in a loop,
     * and finally closes the producer.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {

        Properties props = new Properties();
        // Holds all Kafka producer configuration properties

        props.put("bootstrap.servers", "localhost:9092");
        // Kafka broker address where producer will connect

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Serializer to convert message key to bytes

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Serializer to convert message value to bytes


        // -------- Optional tuning for high throughput --------

        props.put("acks", "1");
        // Producer waits for leader acknowledgment only (faster, less durable)

        props.put("batch.size", 32768);
        // Buffer up to 32 KB of messages before sending a batch

        props.put("linger.ms", 5);
        // Wait up to 5 ms to accumulate more messages into a batch

        props.put("compression.type", "lz4");
        // Compress messages using LZ4 for higher throughput


        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // Create Kafka producer instance with above configuration


        /**
         * Loop to send 1 million messages.
         * Each message contains a key and value formatted as:
         * key-i, value-i
         */
        for (int i = 0; i < 1_000_000; i++) {
            producer.send(new ProducerRecord<>("qwerty", "key-" + i, "value-" + i));
            // Send message to topic "qwerty"
        }


        producer.close();
        // Close producer and flush any pending messages
    }
}