package com.fastcampus.streaming.flinkcourse.chapter3.basic.map;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithCommission;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MapExercise {
    private static final double COMMISSION_RATE = 0.02;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("map-exercise")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "stock-kafka-source");

        DataStream<StockWithCommission> stocksWithCommission = stocks.map(stock -> {
            double priceWithCommission = stock.getPrice() * (1 + COMMISSION_RATE);
            return new StockWithCommission(stock.getSymbol(), priceWithCommission, stock.getTimestamp(), COMMISSION_RATE);
        });

        stocksWithCommission.print();

        env.execute("Stock Price with Commission");
    }
}
