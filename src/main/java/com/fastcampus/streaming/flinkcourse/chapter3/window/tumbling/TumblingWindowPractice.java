package com.fastcampus.streaming.flinkcourse.chapter3.window.tumbling;

import com.fastcampus.streaming.flinkcourse.chapter3.window.tumbling.conversion.TumblingWindowPracticeProcessWindowFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.window.tumbling.function.StockWithCountAveragePriceReduceFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class TumblingWindowPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("tumbling-window-practice")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                        .withIdleness(Duration.ofMillis(500)),
                "stock-count-kafka-source");

        DataStream<StockWithCount> stocksWithCount = stocks.map(
                stock -> new StockWithCount(stock.getSymbol(), stock.getPrice(), stock.getTimestamp(), 1));

        DataStream<StockWithCount> averageStockPrices = stocksWithCount
                .keyBy(StockWithCount::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .process(new TumblingWindowPracticeProcessWindowFunction());

        averageStockPrices.print();

        env.execute("Tumbling Window Practice");
    }
}
