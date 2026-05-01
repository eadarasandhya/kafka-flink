package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;
import java.util.Properties;

/**
 * KafkaToKafkaProduction
 *
 * A production‑grade Flink streaming job that:
 *  - Reads messages from a Kafka topic
 *  - Applies a 1‑minute tumbling window
 *  - Batches and aggregates messages
 *  - Writes results to another Kafka topic with EXACTLY‑ONCE semantics
 *
 * Demonstrates:
 *  - Flink 2.x checkpointing configuration
 *  - KafkaSource and KafkaSink setup
 *  - Transactional Kafka producer for exactly‑once delivery
 *  - Processing‑time windowing and reduce aggregation
 */
public class KafkaToKafkaProduction {

    /**
     * Main entry point for the Flink job.
     * Configures checkpointing, Kafka source, Kafka sink,
     * windowing logic, and executes the pipeline.
     *
     * @param args command‑line arguments (unused)
     * @throws Exception if the Flink job fails
     */
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Create Flink execution environment

        // --------------------------------------------------------------------
        // 1. EXACTLY-ONCE CHECKPOINTING (Flink 2.x style)
        // --------------------------------------------------------------------
        env.enableCheckpointing(10000); // 10 seconds
        // Enable periodic checkpoints

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Enable exactly‑once processing guarantees

        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
        );
        // Retain checkpoints even if job is cancelled

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // Minimum pause between checkpoints

        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // Timeout for checkpoint completion

        // --------------------------------------------------------------------
        // 2. KAFKA SOURCE
        // --------------------------------------------------------------------
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("172.18.0.4:9092")  // Kafka broker
                .setTopics("qwerty")                     // Input topic
                .setGroupId("demo-group")                // Consumer group
                .setStartingOffsets(OffsetsInitializer.earliest()) // Start from earliest
                .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize values as strings
                .build();
        // Build Kafka source

        // --------------------------------------------------------------------
        // 3. KAFKA SINK (EXACTLY-ONCE)
        // --------------------------------------------------------------------
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("transaction.timeout.ms", "900000"); // 15 minutes
        // Required for long-running Flink transactions

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("172.18.0.2:29092")  // Kafka broker for output
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("batch-results")  // Output topic
                                .setValueSerializationSchema(new SimpleStringSchema()) // Serialize values
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // Enable exactly-once
                .setTransactionalIdPrefix("flink-producer") // Required for Kafka transactions
                .setKafkaProducerConfig(kafkaProducerProps) // Custom producer configs
                .build();
        // Build Kafka sink

        // --------------------------------------------------------------------
        // 4. PIPELINE LOGIC
        // --------------------------------------------------------------------
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Input")
                .map(val -> "Batch Message: " + val)  // Simple transformation
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1))) // 1-minute window
                .reduce((v1, v2) -> v1 + " | " + v2)  // Batch aggregation
                .sinkTo(sink);  // Write to Kafka sink

        env.execute("Kafka-to-Kafka Production Batching");
        // Execute Flink job
    }
}