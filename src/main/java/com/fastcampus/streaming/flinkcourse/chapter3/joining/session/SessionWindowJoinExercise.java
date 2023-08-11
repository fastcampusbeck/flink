package com.fastcampus.streaming.flinkcourse.chapter3.joining.session;

import com.fastcampus.streaming.flinkcourse.chapter3.joining.session.source.NewsSource;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.news.News;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class SessionWindowJoinExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties stockProperties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("session-window-join-exercise")
                .build();
        KafkaSource<Stock> stockKafkaSource = KafkaSourceSink.createSource(stockProperties, Stock.class);
        DataStream<Stock> stocks = env.fromSource(
                stockKafkaSource, WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((stock, l) -> stock.getTimestamp())
                        .withIdleness(Duration.ofMillis(500)),
                "stock-kafka-source");

        DataStream<News> news = env.addSource(new NewsSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<News>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((article, l) -> article.getTimestamp())
                        .withIdleness(Duration.ofMillis(500)));

        DataStream<Stock> microsoftStocks = stocks
                .filter(stock -> stock.getSymbol().equals("MSFT"));

        DataStream<News> negativeGoogleNews = news
                .filter(article -> article.getSymbol().equals("GOOGL") && !article.getIsPositive());

        microsoftStocks.join(negativeGoogleNews)
                .where(stock -> "dummy")
                .equalTo(article -> "dummy")
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(500)))
                .apply((stock, article) -> "Negative news about " + article.getSymbol() + " titled '" + article.getTitle() +
                        "' coincided with a trade of " + stock.getSymbol() + " stock at a price of " + stock.getPrice())
                .print();

        env.execute("Session Window Join Exercise");
    }
}
