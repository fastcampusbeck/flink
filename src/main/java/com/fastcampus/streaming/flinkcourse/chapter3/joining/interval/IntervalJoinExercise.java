package com.fastcampus.streaming.flinkcourse.chapter3.joining.interval;

import com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.source.RatingChangeEventSource;
import com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.function.RatingImpactProcessJoinFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.source.TradeSource;
import com.fastcampus.streaming.flinkcourse.chapter3.joining.session.source.TradingActivitySource;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.trade.RatingChange;
import com.fastcampus.streaming.flinkcourse.model.trade.Trade;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class IntervalJoinExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Trade> trades = env.addSource(new TradeSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Trade>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ratingChange, l) -> ratingChange.getTimestamp())
                                .withIdleness(Duration.ofSeconds(1)));

        DataStream<RatingChange> ratingChanges = env.addSource(new RatingChangeEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RatingChange>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ratingChange, l) -> ratingChange.getTimestamp())
                        .withIdleness(Duration.ofSeconds(1)));

        trades.keyBy(Trade::getSecurityId)
                .intervalJoin(ratingChanges.keyBy(RatingChange::getSecurityId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new RatingImpactProcessJoinFunction())
                .print();

        env.execute("Credit Rating Impact Analysis");
    }
}
