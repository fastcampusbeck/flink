package com.fastcampus.streaming.flinkcourse.kafka.config.factory.primitive;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSinkHelper;
import com.fastcampus.streaming.flinkcourse.kafka.config.factory.SourceFactory;
import com.fastcampus.streaming.flinkcourse.kafka.serde.string.StringKafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;

import java.util.Properties;

public class StringSourceFactory implements SourceFactory<String> {
    @Override
    public KafkaSource<String> createSource(Properties properties) {
        return KafkaSourceSinkHelper.createKafkaSource(properties, new StringKafkaRecordDeserializationSchema());
    }
}
