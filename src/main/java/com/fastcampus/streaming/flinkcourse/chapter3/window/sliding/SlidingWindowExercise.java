package com.fastcampus.streaming.flinkcourse.chapter3.window.sliding;

import com.fastcampus.streaming.flinkcourse.chapter3.window.sliding.function.AverageStockPriceAggregateFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.aggregate.AverageStockPrice;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class SlidingWindowExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("sliding-window-exercise")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource,
                WatermarkStrategy
                        .<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                        .withIdleness(Duration.ofMillis(500)),
                "stock-kafka-source");

        DataStream<AverageStockPrice> averageStockPrices = stocks
                .keyBy(Stock::getSymbol)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .aggregate(new AverageStockPriceAggregateFunction());

        averageStockPrices.print();

        env.execute("Sliding Window Exercise");
    }
}
