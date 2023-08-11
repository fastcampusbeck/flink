package com.fastcampus.streaming.flinkcourse.chapter3.iterate;

import com.fastcampus.streaming.flinkcourse.chapter3.iterate.function.CalculateEMAConvergedMapFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithConverged;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public class IterateExerciseImprove {
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

        DataStream<StockWithConverged> stocksWithConverged = stocks.map(stock ->
                new StockWithConverged(stock.getSymbol(), stock.getPrice(), stock.getTimestamp(), false));

        IterativeStream<StockWithConverged> iteration = stocksWithConverged.iterate();

        DataStream<StockWithConverged> iterationBody = iteration.map(new CalculateEMAConvergedMapFunction(10, 0.001));

        DataStream<StockWithConverged> feedback = iterationBody.filter(stock -> !stock.getConverged());
        DataStream<StockWithConverged> output = iterationBody.filter(stock -> stock.getConverged());

        iteration.closeWith(feedback);

        output.print();

        env.execute("Iterative Converging EMA Calculation");
    }
}
