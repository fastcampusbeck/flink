package com.fastcampus.streaming.flinkcourse.chapter3.window.calculate;

import com.fastcampus.streaming.flinkcourse.chapter3.window.calculate.function.StockVolatilityCalculatorProcessWindowFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class ProcessExampleFunction {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("process-window-function")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                        .withIdleness(Duration.ofSeconds(3)),
                "stock-kafka-source");

        stocks.keyBy(Stock::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new StockVolatilityCalculatorProcessWindowFunction())
                .name("Stock Volatility")
                .print();

        env.execute("Real-time Stock Volatility Analysis (Process)");
    }
}
