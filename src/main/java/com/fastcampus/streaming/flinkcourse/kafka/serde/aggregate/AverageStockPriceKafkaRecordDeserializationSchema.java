package com.fastcampus.streaming.flinkcourse.kafka.serde.aggregate;

import com.fastcampus.streaming.flinkcourse.model.aggregate.AverageStockPrice;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class AverageStockPriceKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<AverageStockPrice> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<AverageStockPrice> collector) throws IOException {
        collector.collect(objectMapper.readValue(consumerRecord.value(), AverageStockPrice.class));
    }

    @Override
    public TypeInformation<AverageStockPrice> getProducedType() {
        return null;
    }
}
