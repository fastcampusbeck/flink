package com.fastcampus.streaming.flinkcourse.chapter3.basic.keyby;

import com.fastcampus.streaming.flinkcourse.chapter3.basic.keyby.function.StockCountMapFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class KeyByPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("key-by-practice")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "stock-kafka-source");

        DataStream<StockWithCount> stockCounts = stocks
                .keyBy(Stock::getSymbol)
                .map(new StockCountMapFunction());

        stockCounts.print();

        env.execute("Stock Count");
    }
}
