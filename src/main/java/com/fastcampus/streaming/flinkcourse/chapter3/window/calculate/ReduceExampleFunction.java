package com.fastcampus.streaming.flinkcourse.chapter3.window.calculate;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class ReduceExampleFunction {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-transactions")
                .setGroupId("reduce-window-function")
                .build();

        KafkaSource<StockTransaction> kafkaSource = KafkaSourceSink.createSource(properties, StockTransaction.class);

        DataStream<StockTransaction> stocks = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<StockTransaction>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                        .withIdleness(Duration.ofSeconds(3)),
                "stock-transaction-kafka-source");

        stocks.keyBy(StockTransaction::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((stockTransaction1, stockTransaction2) ->
                        new StockTransaction(
                                stockTransaction1.getSymbol(), stockTransaction1.getPrice(),
                                Math.max(stockTransaction1.getTimestamp(), stockTransaction2.getTimestamp()),
                                stockTransaction1.getVolume() + stockTransaction2.getVolume()
                        ))
                .name("Cumulative Stock Volume")
                .print();

        env.execute("Real-time Stock Volume Analysis (Reduce)");
    }
}
