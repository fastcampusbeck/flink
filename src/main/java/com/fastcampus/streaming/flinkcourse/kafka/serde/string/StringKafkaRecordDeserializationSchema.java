package com.fastcampus.streaming.flinkcourse.kafka.serde.string;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class StringKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<String> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<String> collector) throws IOException {
        collector.collect(new String(consumerRecord.value()));
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(String.class);
    }
}
