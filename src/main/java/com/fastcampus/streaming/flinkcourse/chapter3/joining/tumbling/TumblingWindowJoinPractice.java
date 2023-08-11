package com.fastcampus.streaming.flinkcourse.chapter3.joining.tumbling;

import com.fastcampus.streaming.flinkcourse.chapter3.joining.tumbling.function.ClosingPriceProcessWindowFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

import java.time.Duration;
import java.util.Properties;

public class TumblingWindowJoinPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties nyseProperties = new KafkaProperties()
                .setSourceTopic("nyse-stock-transactions")
                .setGroupId("tumbling-window-join-practice")
                .build();
        KafkaSource<StockTransaction> nyseKafkaSource = KafkaSourceSink.createSource(nyseProperties, StockTransaction.class);
        DataStream<StockTransaction> nyseStockTransactions = env.fromSource(
                nyseKafkaSource,
                WatermarkStrategy
                        .<StockTransaction>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()).withIdleness(Duration.ofMillis(500)),
                "nyse-stock-transaction-kafka-source");

        DataStream<Stock> nyseStocks = nyseStockTransactions.map(stockTransaction ->
                new Stock(stockTransaction.getSymbol(), stockTransaction.getPrice(), stockTransaction.getTimestamp()));

        Properties nasdaqProperties = new KafkaProperties()
                .setSourceTopic("nasdaq-stock-transactions")
                .setGroupId("tumbling-window-join-exercise")
                .build();
        KafkaSource<StockTransaction> nasdaqKafkaSource = KafkaSourceSink.createSource(nasdaqProperties, StockTransaction.class);
        DataStream<StockTransaction> nasdaqStockTransactions = env.fromSource(
                nasdaqKafkaSource,
                WatermarkStrategy
                        .<StockTransaction>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()).withIdleness(Duration.ofMillis(500)),
                "nasdaq-stock-transaction-kafka-source");

        DataStream<Stock> nasdaqStocks = nasdaqStockTransactions.map(stockTransaction ->
                new Stock(stockTransaction.getSymbol(), stockTransaction.getPrice(), stockTransaction.getTimestamp()));

        DataStream<Stock> lastHighPriceStock = nasdaqStocks.join(nyseStocks)
                .where(Stock::getSymbol)
                .equalTo(Stock::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((first, second) -> new Stock(first.getSymbol(),
                        Math.max(first.getPrice(), second.getPrice()),
                        (first.getPrice() >= second.getPrice()) ? first.getTimestamp() : second.getTimestamp()));

        DataStream<Stock> closingStocks = lastHighPriceStock.keyBy(Stock::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ClosingPriceProcessWindowFunction());

        DataStream<Stock> maxPriceStocks = closingStocks.keyBy(Stock::getSymbol)
                .window(GlobalWindows.create())
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                .reduce((stock, t1) -> stock.getPrice() > t1.getPrice() ? stock : t1);

        maxPriceStocks.print();

        env.execute("Tumbling Window Join Practice");
    }
}
