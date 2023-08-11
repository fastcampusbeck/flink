package com.fastcampus.streaming.flinkcourse.kafka.config.factory.stock;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSinkHelper;
import com.fastcampus.streaming.flinkcourse.kafka.config.factory.SourceFactory;
import com.fastcampus.streaming.flinkcourse.kafka.serde.stock.StockWithCountClassKafkaRecordDeserializationSchema;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithCount;
import org.apache.flink.connector.kafka.source.KafkaSource;

import java.util.Properties;

public class StockWithCountSourceFactory implements SourceFactory<StockWithCount> {
    @Override
    public KafkaSource<StockWithCount> createSource(Properties properties) {
        return KafkaSourceSinkHelper.createKafkaSource(properties, new StockWithCountClassKafkaRecordDeserializationSchema());
    }
}
