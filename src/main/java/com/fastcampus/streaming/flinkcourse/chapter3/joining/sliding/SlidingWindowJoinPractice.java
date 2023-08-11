package com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding;

import com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.function.StockToStockVolatilityProcessWindowFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.window.StockVolatility;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class SlidingWindowJoinPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("sliding-window-join-practice")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((stock, l) -> stock.getTimestamp())
                        .withIdleness(Duration.ofMillis(500)),
                "stock-kafka-source");

        DataStream<Stock> appleStocks = stocks
                .filter(stock -> stock.getSymbol().equals("AAPL"));

        DataStream<Stock> microsoftStocks = stocks
                .filter(stock -> stock.getSymbol().equals("MSFT"));

        DataStream<StockVolatility> appleStockVolatility = appleStocks.keyBy(Stock::getSymbol)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .process(new StockToStockVolatilityProcessWindowFunction());
        appleStockVolatility.print();
        // a1, a2, ...

        DataStream<StockVolatility> microsoftStockVolatility = microsoftStocks.keyBy(Stock::getSymbol)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .process(new StockToStockVolatilityProcessWindowFunction());
        microsoftStockVolatility.print();
        // m1, m2, ...

        DataStream<Tuple2<StockVolatility, StockVolatility>> comparedStream = appleStockVolatility.join(microsoftStockVolatility)
                .where(StockVolatility::getWindowEnd)
                .equalTo(StockVolatility::getWindowEnd)
                .window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(1)))
                .apply(new JoinFunction<>() {
                    @Override
                    public Tuple2<StockVolatility, StockVolatility> join(StockVolatility first, StockVolatility second) {
                        return new Tuple2<>(first, second);
                    }
                });
        // (a1, m1), (a2, m2)
        // ...
        comparedStream.print();

        DataStream<String> alerts = comparedStream
                .filter((FilterFunction<Tuple2<StockVolatility, StockVolatility>>) volatilityComparison -> {
                    double firstVolatility = volatilityComparison.f0.getVolatility();
                    double secondVolatility = volatilityComparison.f1.getVolatility();

                    double volatilityDifference = Math.abs(firstVolatility - secondVolatility);
                    double averageVolatility = (firstVolatility + secondVolatility) / 2.0;

                    return volatilityDifference > 0.5 * averageVolatility;
                }).map((MapFunction<Tuple2<StockVolatility, StockVolatility>, String>) volatilityComparison
                        -> "Alert! Significant difference in volatility between " + volatilityComparison.f0.getSymbol() +
                        " and " + volatilityComparison.f1.getSymbol());
        alerts.print();
        // A, b, C
        // b, C, D
        // C, D, E


        env.execute("Sliding Window Join Practice");
    }
}
