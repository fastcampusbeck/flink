package com.fastcampus.streaming.flinkcourse.chapter3.window.sliding;

import com.fastcampus.streaming.flinkcourse.chapter3.window.sliding.conversion.SlidingWindowPracticeProcessWindowFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.window.sliding.function.StockTransactionToTotalTradedVolumeMapFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.window.sliding.function.StockVolumeReduceFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.aggregate.TotalTradedVolume;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class SlidingWindowPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-transactions")
                .setGroupId("sliding-window-practice")
                .build();

        KafkaSource<StockTransaction> kafkaSource = KafkaSourceSink.createSource(properties, StockTransaction.class);

        DataStream<StockTransaction> stockTransactions = env.fromSource(
                kafkaSource,
                WatermarkStrategy
                        .<StockTransaction>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                        .withIdleness(Duration.ofMillis(500)),
                "stock-transaction-kafka-source");

        DataStream<TotalTradedVolume> averageStockPrices = stockTransactions
                .keyBy(StockTransaction::getSymbol)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .process(new SlidingWindowPracticeProcessWindowFunction());

        averageStockPrices.print();

        env.execute("Stock Price with Commission");
    }
}
