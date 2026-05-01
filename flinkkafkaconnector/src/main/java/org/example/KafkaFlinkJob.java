package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KafkaFlinkJob
 *
 * A simple Flink streaming job that:
 *  - Connects to a Kafka topic
 *  - Reads messages with no watermarking
 *  - Prints messages to stdout
 *  - Uses exactly-once checkpointing for reliability
 *
 * Demonstrates:
 *  - Basic KafkaSource setup
 *  - Flink checkpoint configuration
 *  - Minimal streaming pipeline
 */
public class KafkaFlinkJob {

    /**
     * Main entry point for the application.
     * Delegates execution to flinkKafkaProcess().
     *
     * @param args command-line arguments (unused)
     * @throws Exception if the Flink job fails
     */
    public static void main(String[] args) throws Exception {
        flinkKafkaProcess();
    }

    /**
     * Builds and executes a Flink streaming pipeline that:
     *  - Enables exactly-once checkpointing
     *  - Reads from Kafka topic "qwerty"
     *  - Prints all consumed messages
     *
     * Includes recommended checkpointing settings for stability.
     *
     * @throws Exception if the Flink job execution fails
     */
    protected static void flinkKafkaProcess() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Create Flink execution environment

        env.enableCheckpointing(60000); // every 60 seconds
        // Enable periodic checkpointing

        env.getCheckpointConfig().setCheckpointingMode(
                org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        // Enable exactly-once processing

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        // Minimum pause between checkpoints

        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // Timeout for checkpoint completion

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // Allow only one checkpoint at a time

        // Kafka Source configuration
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("172.18.0.2:29092")  // Kafka broker
                .setTopics("qwerty")                      // Topic name
                .setGroupId("demo-group")                 // Consumer group
                .setStartingOffsets(OffsetsInitializer.earliest()) // Read from earliest
                .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize values
                .build();

        DataStreamSource<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // Create stream from Kafka source

        stream.print();
        // Print messages to stdout

        env.execute("Flink Kafka Reader - qwerty");
        // Execute Flink job
    }

}