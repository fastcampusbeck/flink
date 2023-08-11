package com.fastcampus.streaming.flinkcourse.kafka.producer;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.generator.SampleDataGenerator;
import com.fastcampus.streaming.flinkcourse.kafka.producer.factory.DataGeneratorFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class SampleDataProducer {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Please provide a generator type as an argument");
        }

        // Get the generator type from the command-line arguments
        // "StockSingle", "StockGroup", "StockSector", "StockTransaction", "PortfolioSingle"
        String generatorType = args[0];

        // Create producer properties
        Properties properties = new KafkaProperties().build();

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a DataGenerator and generate and send data
        SampleDataGenerator dataGenerator = DataGeneratorFactory.createDataGenerator(producer, generatorType);
        dataGenerator.generateAndSend();

        // Flush and close producer
        producer.flush();
        producer.close();
    }
}
