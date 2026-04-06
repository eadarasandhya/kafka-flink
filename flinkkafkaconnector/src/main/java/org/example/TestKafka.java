package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TestKafka {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// Optional tuning for high throughput
        props.put("acks", "1");
        props.put("batch.size", 32768);
        props.put("linger.ms", 5);
        props.put("compression.type", "lz4");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 1_000_000; i++) {
            producer.send(new ProducerRecord<>("qwerty", "key-" + i, "value-" + i));
        }

        producer.close();
    }
}
