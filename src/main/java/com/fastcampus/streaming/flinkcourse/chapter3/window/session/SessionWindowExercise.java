package com.fastcampus.streaming.flinkcourse.chapter3.window.session;

import com.fastcampus.streaming.flinkcourse.chapter3.window.session.function.RealizedVolatilityCalculatorProcessWindowFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class SessionWindowExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("session-window-exercise")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofMillis(500))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                        .withIdleness(Duration.ofMillis(250)),
                "stock-kafka-source");

        DataStream<String> realizedVolatility = stocks
                .keyBy(Stock::getSymbol)
                .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
                .process(new RealizedVolatilityCalculatorProcessWindowFunction());

        realizedVolatility.print();

        env.execute("Real-time Realized Volatility Calculation");
    }
}
