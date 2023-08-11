package com.fastcampus.streaming.flinkcourse.chapter3.basic.filter;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithWatchlist;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FilterPractice {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("filter-practice")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "stock-kafka-source");

        DataStream<StockWithWatchlist> watchlistStocks = stocks.map(stock -> new StockWithWatchlist(stock.getSymbol(), stock.getPrice(), stock.getTimestamp()))
                .returns(StockWithWatchlist.class)
                .filter(stockWithWatchlist -> stockWithWatchlist.getWatchlist().contains(stockWithWatchlist.getSymbol()));

        watchlistStocks.print();

        env.execute("Watchlist Filter Stock");
    }
}
