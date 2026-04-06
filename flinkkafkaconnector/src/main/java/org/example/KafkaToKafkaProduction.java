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

public class KafkaToKafkaProduction {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --------------------------------------------------------------------
        // 1. EXACTLY-ONCE CHECKPOINTING (Flink 2.x style)
        // --------------------------------------------------------------------
        env.enableCheckpointing(10000); // 10 seconds
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Keep checkpoints even if job is cancelled
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
        );

        // Recommended stability settings
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // --------------------------------------------------------------------
        // 2. KAFKA SOURCE
        // --------------------------------------------------------------------
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("172.18.0.4:9092")
                .setTopics("qwerty")
                .setGroupId("demo-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // --------------------------------------------------------------------
        // 3. KAFKA SINK (EXACTLY-ONCE)
        // --------------------------------------------------------------------
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("transaction.timeout.ms", "900000"); // 15 minutes

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("172.18.0.2:29092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("batch-results")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flink-producer")
                .setKafkaProducerConfig(kafkaProducerProps)
                .build();

        // --------------------------------------------------------------------
        // 4. PIPELINE LOGIC
        // --------------------------------------------------------------------
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Input")
                .map(val -> "Batch Message: " + val)
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
                .reduce((v1, v2) -> v1 + " | " + v2)
                .sinkTo(sink);

        env.execute("Kafka-to-Kafka Production Batching");
    }
}