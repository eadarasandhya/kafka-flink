package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * KafkaToS3Parquet
 *
 * A Flink streaming job that reads messages from Kafka, performs
 * 1‑minute tumbling window aggregation, and writes aggregated results
 * to S3 in Parquet format using Avro reflection.
 *
 * Demonstrates:
 * - KafkaSource ingestion
 * - Processing-time windowing
 * - Custom window aggregation
 * - Parquet bulk sink with S3A filesystem
 * - Exactly-once checkpointing
 */
public class KafkaToS3Parquet {

    /**
     * POJO representing a single aggregated window record.
     * Used as the Parquet schema via Avro reflection.
     */
    public static class WindowRecord {
        public String windowStart;
        public String windowEnd;
        public long count;
        public String aggregateData;

        public WindowRecord() {}

        /**
         * Constructs a window aggregation record.
         *
         * @param start ISO timestamp of window start
         * @param end ISO timestamp of window end
         * @param count number of messages in the window
         * @param data concatenated message payloads
         */
        public WindowRecord(String start, String end, long count, String data) {
            this.windowStart = start;
            this.windowEnd = end;
            this.count = count;
            this.aggregateData = data;
        }
    }

    /**
     * Main entry point for the Flink job.
     * Configures Kafka source, windowing logic, Parquet sink,
     * and executes the streaming pipeline.
     *
     * @param args command-line arguments (unused)
     * @throws Exception if the Flink job fails
     */
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Flink execution environment for building the pipeline

        // Enable exactly-once checkpointing every 1 minute
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // ---------------------- Kafka Source ----------------------

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("172.18.0.4:9092")   // Kafka broker
                .setTopics("qwerty")                      // Input topic
                .setGroupId("demo-group")                 // Consumer group
                .setStartingOffsets(OffsetsInitializer.earliest()) // Read from beginning
                .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize as String
                .build();

        // ---------------------- Window Aggregation ----------------------

        /**
         * 1-minute tumbling window over all messages.
         * Aggregates:
         * - window start/end timestamps
         * - count of messages
         * - concatenated payloads
         */
        SingleOutputStreamOperator<WindowRecord> windowed = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Input")
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
                .process(new ProcessAllWindowFunction<String, WindowRecord, TimeWindow>() {

                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<WindowRecord> out) {

                        DateTimeFormatter fmt =
                                DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

                        long count = 0;
                        StringBuilder agg = new StringBuilder();

                        // Aggregate all messages in the window
                        for (String v : elements) {
                            if (agg.length() > 0) agg.append(" | ");
                            agg.append(v);
                            count++;
                        }

                        // Convert window timestamps to ISO-8601
                        String start = fmt.format(Instant.ofEpochMilli(context.window().getStart()));
                        String end = fmt.format(Instant.ofEpochMilli(context.window().getEnd()));

                        // Emit aggregated window record
                        out.collect(new WindowRecord(start, end, count, agg.toString()));
                    }
                });

        // ---------------------- S3 Parquet Sink ----------------------

        String s3Path = "s3a://kafka-flink-poc-s3/flink-output/";

        /**
         * Parquet sink using:
         * - Avro reflection for schema
         * - Date-based bucket partitioning
         * - On-checkpoint rolling (required for Parquet)
         * - Custom file prefix/suffix
         */
        final FileSink<WindowRecord> parquetSink = FileSink
                .forBulkFormat(new Path(s3Path), AvroParquetWriters.forReflectRecord(WindowRecord.class))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'date='yyyy-MM-dd"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("part")
                        .withPartSuffix(".parquet")
                        .build())
                .build();

        // Attach sink to pipeline
        windowed.sinkTo(parquetSink);

        // Execute Flink job
        env.execute("Kafka-to-S3 Parquet 1min");
    }
}