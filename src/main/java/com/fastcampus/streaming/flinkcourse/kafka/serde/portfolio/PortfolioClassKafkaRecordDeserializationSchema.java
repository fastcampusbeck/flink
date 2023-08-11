package com.fastcampus.streaming.flinkcourse.kafka.serde.portfolio;

import com.fastcampus.streaming.flinkcourse.model.portfolio.Portfolio;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class PortfolioClassKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<Portfolio> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Portfolio> collector) throws IOException {
        collector.collect(objectMapper.readValue(consumerRecord.value(), Portfolio.class));
    }

    @Override
    public TypeInformation<Portfolio> getProducedType() {
        return TypeInformation.of(Portfolio.class);
    }
}
