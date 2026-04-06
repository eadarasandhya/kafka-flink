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

public class KafkaToS3Parquet {

    public static class WindowRecord {
        public String windowStart;
        public String windowEnd;
        public long count;
        public String aggregateData;

        public WindowRecord() {}

        public WindowRecord(String start, String end, long count, String data) {
            this.windowStart = start;
            this.windowEnd = end;
            this.count = count;
            this.aggregateData = data;
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint every 1 minute
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("172.18.0.4:9092")
                .setTopics("qwerty")
                .setGroupId("demo-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 1-minute window aggregation
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

                        for (String v : elements) {
                            if (agg.length() > 0) agg.append(" | ");
                            agg.append(v);
                            count++;
                        }

                        String start = fmt.format(Instant.ofEpochMilli(context.window().getStart()));
                        String end = fmt.format(Instant.ofEpochMilli(context.window().getEnd()));

                        out.collect(new WindowRecord(start, end, count, agg.toString()));
                    }
                });

        // S3 Parquet Sink
        String s3Path = "s3a://kafka-flink-poc-s3/flink-output/";

        final FileSink<WindowRecord> parquetSink = FileSink
                .forBulkFormat(new Path(s3Path), AvroParquetWriters.forReflectRecord(WindowRecord.class))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'date='yyyy-MM-dd"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build()) // REQUIRED for Parquet
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("part")
                        .withPartSuffix(".parquet")
                        .build())
                .build();

        windowed.sinkTo(parquetSink);

        env.execute("Kafka-to-S3 Parquet 1min");
    }
}