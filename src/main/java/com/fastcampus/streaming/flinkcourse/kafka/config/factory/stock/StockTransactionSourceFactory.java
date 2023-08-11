package com.fastcampus.streaming.flinkcourse.kafka.config.factory.stock;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSinkHelper;
import com.fastcampus.streaming.flinkcourse.kafka.config.factory.SourceFactory;
import com.fastcampus.streaming.flinkcourse.kafka.serde.stock.StockTransactionClassKafkaRecordDeserializationSchema;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.connector.kafka.source.KafkaSource;

import java.util.Properties;

public class StockTransactionSourceFactory implements SourceFactory<StockTransaction> {
    @Override
    public KafkaSource<StockTransaction> createSource(Properties properties) {
        return KafkaSourceSinkHelper.createKafkaSource(properties, new StockTransactionClassKafkaRecordDeserializationSchema());
    }
}
