package com.fastcampus.streaming.flinkcourse.chapter2.serde;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class StockCSVKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<Stock> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Stock> collector) throws IOException {
        String[] parts = new String(consumerRecord.value()).split(",");
        collector.collect(new Stock(parts[0], Double.parseDouble(parts[1]), Long.parseLong(parts[2])));
    }

    @Override
    public TypeInformation<Stock> getProducedType() {
        return TypeInformation.of(Stock.class);
    }
}
