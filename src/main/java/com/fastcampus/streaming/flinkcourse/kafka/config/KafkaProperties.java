package com.fastcampus.streaming.flinkcourse.kafka.config;

import java.util.Properties;

public class KafkaProperties {
    private final Properties properties;

    public KafkaProperties() {
        this.properties = new Properties();
        this.properties.setProperty("bootstrap.servers", "localhost:9092");
        this.properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public KafkaProperties setSourceTopic(String topic) {
        this.properties.setProperty("topic.source", topic);
        return this;
    }

    public KafkaProperties setGroupId(String groupId) {
        this.properties.setProperty("group-id", groupId);
        return this;
    }

    public Properties build() {
        return this.properties;
    }
}
