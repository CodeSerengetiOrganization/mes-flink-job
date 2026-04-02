package org.codeserengeti.mes.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the MES Flink streaming job.
 *
 * <p>This job reads manufacturing event messages from a Kafka topic, applies
 * a simple transformation, and prints the results to standard output.
 * Replace the sink and processing logic with your production requirements.
 */
public class MesFlinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(MesFlinkJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildPipeline(env);
        env.execute("MES Flink Job");
    }

    /**
     * Constructs the Flink pipeline on the given execution environment.
     *
     * @param env the Flink {@link StreamExecutionEnvironment}
     */
    public static void buildPipeline(StreamExecutionEnvironment env) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String inputTopic = System.getenv().getOrDefault("KAFKA_INPUT_TOPIC", "mes-events");
        String groupId = System.getenv().getOrDefault("KAFKA_GROUP_ID", "mes-flink-job");

        LOG.info("Connecting to Kafka at {} reading topic '{}'", bootstrapServers, inputTopic);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        stream
                .map(new ProcessEventFunction())
                .print();
    }

    /**
     * Simple processing function that transforms each incoming MES event message.
     * Replace this with domain-specific processing logic.
     */
    static class ProcessEventFunction implements MapFunction<String, String> {
        @Override
        public String map(String value) {
            return "[MES] " + value;
        }
    }
}
