package com.fastcampus.streaming.flinkcourse.kafka.config.factory;

import org.apache.flink.connector.kafka.source.KafkaSource;

import java.util.Properties;

public interface SourceFactory<T> {
    KafkaSource<T> createSource(Properties properties);
}
