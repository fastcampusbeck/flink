package com.fastcampus.streaming.flinkcourse.kafka.generator;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Future;

public abstract class AbstractSampleDataGenerator implements SampleDataGenerator {
    protected final KafkaProducer<String, String> producer;
    protected final ObjectMapper objectMapper;
    protected final Random random;

    public AbstractSampleDataGenerator(KafkaProducer<String, String> producer) {
        this.producer = producer;
        this.objectMapper = new ObjectMapper();
        this.random = new Random();
    }

    protected void sendRecord(String topic, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);

        // Send data - asynchronous
        Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
            // Wrap the exception in an Optional
            Optional<Exception> optionalException = Optional.ofNullable(exception);

            // If the Optional is empty (i.e., there's no exception), the record was successfully sent
            optionalException.ifPresentOrElse(Throwable::printStackTrace, () -> System.out.println("Success!"));
        });
    }
}
