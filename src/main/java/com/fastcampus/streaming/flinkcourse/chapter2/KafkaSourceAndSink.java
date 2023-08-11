package com.fastcampus.streaming.flinkcourse.chapter2;

import com.fastcampus.streaming.flinkcourse.chapter2.serde.StockCSVKafkaRecordDeserializationSchema;
import com.fastcampus.streaming.flinkcourse.chapter2.serde.StockClassKafkaRecordSerializationSchema;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSourceAndSink {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_INPUT_TOPIC = "source-test";
    private static final String DEFAULT_OUTPUT_TOPIC = "sink-test";
    private static final String DEFAULT_GROUP_ID = "source-sink-test";

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Stock> kafkaSource = createKafkaSource(parameterTool);

        DataStream<Stock> stocks = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "stock-kafka-source");
        stocks.print();

        stocks.sinkTo(createKafkaSink(parameterTool));

        env.execute("Kafka Source to Sink Example");
    }

    private static KafkaSource<Stock> createKafkaSource(ParameterTool parameterTool) {
        String bootstrapServers = parameterTool.get("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
        String inputTopic = parameterTool.get("input-topic", DEFAULT_INPUT_TOPIC);
        String groupId = parameterTool.get("group-id", DEFAULT_GROUP_ID);
        return KafkaSource.<Stock>builder()
                .setBootstrapServers(bootstrapServers)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setDeserializer(new StockCSVKafkaRecordDeserializationSchema())
                .build();
    }

    private static KafkaSink<Stock> createKafkaSink(ParameterTool parameterTool) {
        String bootstrapServers = parameterTool.get("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
        String outputTopic = parameterTool.get("output-topic", DEFAULT_OUTPUT_TOPIC);

        return KafkaSink.<Stock>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new StockClassKafkaRecordSerializationSchema(outputTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
