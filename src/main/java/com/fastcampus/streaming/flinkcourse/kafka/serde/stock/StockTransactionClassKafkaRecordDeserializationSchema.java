package com.fastcampus.streaming.flinkcourse.kafka.serde.stock;

import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class StockTransactionClassKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<StockTransaction> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<StockTransaction> collector) throws IOException {
        collector.collect(objectMapper.readValue(consumerRecord.value(), StockTransaction.class));
    }

    @Override
    public TypeInformation<StockTransaction> getProducedType() {
        return TypeInformation.of(StockTransaction.class);
    }
}
