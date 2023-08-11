package com.fastcampus.streaming.flinkcourse.kafka.config;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import java.util.Properties;

public class KafkaSourceSinkHelper {
    public static <T> KafkaSource<T> createKafkaSource(Properties properties,
                                                       KafkaRecordDeserializationSchema<T> deserializer) {
        String bootstrapServers = properties.getProperty("bootstrap.servers");
        String sourceTopic = properties.getProperty("topic.source");
        String groupId = properties.getProperty("group-id");

        return KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics(sourceTopic)
                .setGroupId(groupId)
                .setDeserializer(deserializer)
                .build();
    }
}
