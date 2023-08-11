package com.fastcampus.streaming.flinkcourse.kafka.generator.portfolio;

import com.fastcampus.streaming.flinkcourse.kafka.generator.AbstractSampleDataGenerator;
import com.fastcampus.streaming.flinkcourse.model.portfolio.Portfolio;
import org.apache.kafka.clients.producer.KafkaProducer;

public abstract class PortfolioDataGenerator extends AbstractSampleDataGenerator {
    public PortfolioDataGenerator(KafkaProducer<String, String> producer) {
        super(producer);
    }

    protected abstract Portfolio createPortfolio();
}
