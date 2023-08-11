package com.fastcampus.streaming.flinkcourse.chapter3.joining.tumbling;

import com.fastcampus.streaming.flinkcourse.chapter3.joining.tumbling.function.StockTransactionsToTotalTradedVolumeAggregateFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.aggregate.TotalTradedVolume;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import com.fastcampus.streaming.flinkcourse.util.DateTimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;

public class TumblingWindowJoinExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties nyseProperties = new KafkaProperties()
                .setSourceTopic("nyse-stock-transactions")
                .setGroupId("tumbling-window-join-exercise")
                .build();
        KafkaSource<StockTransaction> nyseKafkaSource = KafkaSourceSink.createSource(nyseProperties, StockTransaction.class);
        DataStream<StockTransaction> nyseStockTransactions = env.fromSource(
                nyseKafkaSource,
                WatermarkStrategy
                        .<StockTransaction>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()).withIdleness(Duration.ofMillis(500)),
                "nyse-stock-transaction-kafka-source");

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

        DataStream<TotalTradedVolume> nyseVolume = nyseStockTransactions
                .keyBy(StockTransaction::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new StockTransactionsToTotalTradedVolumeAggregateFunction());

        DataStream<TotalTradedVolume> nasdaqVolume = nasdaqStockTransactions
                .keyBy(StockTransaction::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new StockTransactionsToTotalTradedVolumeAggregateFunction());

        DataStream<TotalTradedVolume> joinedVolume = nyseVolume.join(nasdaqVolume)
                .where(TotalTradedVolume::getSymbol)
                .equalTo(TotalTradedVolume::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((first, second) ->
                        new TotalTradedVolume(first.getSymbol(), first.getTotalVolume() + second.getTotalVolume()));

        joinedVolume.filter(totalTradedVolume -> totalTradedVolume.getSymbol().equals("AAPL"))
                .process(new ProcessFunction<TotalTradedVolume, String>() {
                    @Override
                    public void processElement(TotalTradedVolume totalTradedVolume, ProcessFunction<TotalTradedVolume, String>.Context context, Collector<String> collector) throws Exception {
                        long windowStartTime = context.timestamp() - (context.timestamp() % 5000);
                        long windowEndTime = windowStartTime + 5000;

                        ZonedDateTime windowZonedStartDate = DateTimeConverter.epochMilliToZonedDateTime(windowStartTime, ZoneId.systemDefault());
                        ZonedDateTime windowZonedEndDate = DateTimeConverter.epochMilliToZonedDateTime(windowEndTime, ZoneId.systemDefault());

                        collector.collect("Window start: " + windowZonedStartDate + ", Window end: " + windowZonedEndDate);
                    }
                })
                .print();

        DataStream<String> unusualActivity = joinedVolume
                .filter(totalTradedVolume -> totalTradedVolume.getTotalVolume() > 10000)
                .map(totalTradedVolume -> totalTradedVolume.getSymbol() + " is over 10000, total volume is " + totalTradedVolume.getTotalVolume());
        unusualActivity.print();

        DataStream<String> noUnusualActivity = joinedVolume
                .filter(totalTradedVolume -> totalTradedVolume.getTotalVolume() <= 10000)
                .map(totalTradedVolume -> totalTradedVolume.getSymbol() + " is under 10000, total volume is " + totalTradedVolume.getTotalVolume());
        noUnusualActivity.print();

        env.execute("Tumbling Window Join Example");
    }
}
