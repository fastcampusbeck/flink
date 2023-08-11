package com.fastcampus.streaming.flinkcourse.chapter3.iterate;

import com.fastcampus.streaming.flinkcourse.chapter3.iterate.function.CalculateEMAMapFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public class IterateExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("iterate-exercise")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource, WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((stock, timestamp) -> stock.getTimestamp())
                        .withIdleness(Duration.ofSeconds(1)), "stock-kafka-source");

        IterativeStream<Stock> iteration = stocks.iterate();

        DataStream<Stock> iterationBody = iteration.map(new CalculateEMAMapFunction(10));

        DataStream<Stock> result = iteration.closeWith(iterationBody);

        result.print();

        env.execute("Iterative EMA Calculation");
    }
}
