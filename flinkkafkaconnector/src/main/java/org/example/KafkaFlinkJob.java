package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaFlinkJob {

    public static void main(String[] args) throws Exception {
        flinkKafkaProcess();
    }


        protected static void flinkKafkaProcess() throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(60000); // every 60 seconds
            env.getCheckpointConfig().setCheckpointingMode( org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
            env.getCheckpointConfig().setCheckpointTimeout(60000);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

            // ✅ Use the IP and Internal Port that just worked in your terminal test
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers("172.18.0.2:29092")
                    .setTopics("qwerty")
                    .setGroupId("demo-group")
                    // 👇 Change this to read the existing "test" and "sandhya" messages
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
            stream.print();
            env.execute("Flink Kafka Reader - qwerty");
        }

}