package com.fastcampus.streaming.flinkcourse.kafka.config.factory.stock;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSinkHelper;
import com.fastcampus.streaming.flinkcourse.kafka.config.factory.SourceFactory;
import com.fastcampus.streaming.flinkcourse.kafka.serde.stock.StockClassKafkaRecordDeserializationSchema;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.connector.kafka.source.KafkaSource;

import java.util.Properties;

public class StockSourceFactory implements SourceFactory<Stock> {
    @Override
    public KafkaSource<Stock> createSource(Properties properties) {
        return KafkaSourceSinkHelper.createKafkaSource(properties, new StockClassKafkaRecordDeserializationSchema());
    }
}
