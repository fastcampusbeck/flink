package com.fastcampus.streaming.flinkcourse.chapter3.basic.map;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MapPractice {
    private static final double EXCHANGE_RATE = 0.85;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("map-practice")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "stock-kafka-source");

        DataStream<Stock> stocksInEUR = stocks.map(stock -> {
            double priceInEUR = stock.getPrice() * EXCHANGE_RATE;
            return new Stock(stock.getSymbol(), priceInEUR, stock.getTimestamp());
        });

        stocksInEUR.print();

        env.execute("Stock Price In EUR");
    }
}
