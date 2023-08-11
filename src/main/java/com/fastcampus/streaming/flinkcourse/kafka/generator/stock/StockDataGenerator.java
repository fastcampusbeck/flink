package com.fastcampus.streaming.flinkcourse.kafka.generator.stock;

import com.fastcampus.streaming.flinkcourse.kafka.generator.AbstractSampleDataGenerator;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Arrays;
import java.util.List;


public abstract class StockDataGenerator extends AbstractSampleDataGenerator {
    protected static final List<String> SYMBOLS = Arrays.asList("AAPL", "GOOGL", "MSFT", "AMZN", "FB");

    public StockDataGenerator(KafkaProducer<String, String> producer) {
        super(producer);
    }

    protected abstract Stock createStock();
}
