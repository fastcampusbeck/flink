package com.fastcampus.streaming.flinkcourse.kafka.config.factory.portfolio;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSinkHelper;
import com.fastcampus.streaming.flinkcourse.kafka.config.factory.SourceFactory;
import com.fastcampus.streaming.flinkcourse.kafka.serde.portfolio.PortfolioClassKafkaRecordDeserializationSchema;
import com.fastcampus.streaming.flinkcourse.model.portfolio.Portfolio;
import org.apache.flink.connector.kafka.source.KafkaSource;

import java.util.Properties;

public class PortfolioSourceFactory implements SourceFactory<Portfolio> {
    @Override
    public KafkaSource<Portfolio> createSource(Properties properties) {
        return KafkaSourceSinkHelper.createKafkaSource(properties, new PortfolioClassKafkaRecordDeserializationSchema());
    }
}
