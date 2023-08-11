package com.fastcampus.streaming.flinkcourse.chapter3.basic.keyby;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.aggregate.KeyedStock;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class KeyByExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("key-by-exercise")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "stock-kafka-source");

        KeyedStream<Stock, String> keyByStocks = stocks.keyBy(Stock::getSymbol);

        DataStream<KeyedStock> keyedStocks = keyByStocks
                .map(stock -> new KeyedStock(stock.getSymbol(), stock))
                .returns(KeyedStock.class);

        keyedStocks.print();

        env.execute("Stock KeyBy");
    }
}
