package com.fastcampus.streaming.flinkcourse.chapter2.serde;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class StockClassKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<Stock> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String topic;

    public StockClassKafkaRecordSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Stock stock, KafkaSinkContext kafkaSinkContext, Long aLong) {
        byte[] value;
        try {
            value = objectMapper.writeValueAsBytes(stock);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not serialize record: " + stock, e);
        }
        return new ProducerRecord<>(topic, value);
    }
}