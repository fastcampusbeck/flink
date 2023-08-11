package com.fastcampus.streaming.flinkcourse.kafka.producer.factory;

import com.fastcampus.streaming.flinkcourse.kafka.generator.SampleDataGenerator;
import com.fastcampus.streaming.flinkcourse.kafka.generator.portfolio.PortfolioSingleDataGenerator;
import com.fastcampus.streaming.flinkcourse.kafka.generator.stock.StockGroupDataGenerator;
import com.fastcampus.streaming.flinkcourse.kafka.generator.stock.StockSectorDataGenerator;
import com.fastcampus.streaming.flinkcourse.kafka.generator.stock.StockSingleDataGenerator;
import com.fastcampus.streaming.flinkcourse.kafka.generator.stock.StockTransactionDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;

public class DataGeneratorFactory {
    public static SampleDataGenerator createDataGenerator(KafkaProducer<String, String> producer, String generatorType) {
        switch (generatorType) {
            case "StockSingle":
                return new StockSingleDataGenerator(producer);
            case "StockGroup":
                return new StockGroupDataGenerator(producer);
            case "StockSector":
                return new StockSectorDataGenerator(producer);
            case "StockTransaction":
                return new StockTransactionDataGenerator(producer);
            case "PortfolioSingle":
                return new PortfolioSingleDataGenerator(producer);
            default:
                throw new IllegalArgumentException("Unknown generator type: " + generatorType);
        }
    }
}
