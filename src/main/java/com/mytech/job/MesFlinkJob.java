package com.mytech.job;


import com.mytech.mapper.JsonMapper;
import com.mytech.model.FailedProductBatch;
import com.mytech.model.ManufacturingResult;
import com.mytech.process.FailureFilterFunction;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

public class MesFlinkJob {

    public static void main(String[] args) throws Exception {

        // 1. Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("manufacturing-results-topic")
                .setGroupId("mes-flink-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // 3. Parse JSON → Object
        DataStream<ManufacturingResult> parsed =
                rawStream.map(JsonMapper::toManufacturingResult);

        // 4. Filter failures
        DataStream<ManufacturingResult> failed =
                parsed.filter(new FailureFilterFunction());

        // 5. Map → Failed batch
        DataStream<FailedProductBatch> batchStream =
                failed.map(JsonMapper::toFailedBatch);

        // 6. Convert to JSON
        DataStream<String> outputJson =
                batchStream.map(JsonMapper::toJson);

        // 7. Kafka Sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("mes.failed-product.batch.v1")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        outputJson.sinkTo(sink);

        // 8. Execute
        env.execute("MES Failed Product Processing Job");
    }
}
