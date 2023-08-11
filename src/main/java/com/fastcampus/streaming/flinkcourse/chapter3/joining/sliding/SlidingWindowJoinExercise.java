package com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding;

import com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.function.*;
import com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.source.CommodityDataSource;
import com.fastcampus.streaming.flinkcourse.model.commodity.GoldCommodity;
import com.fastcampus.streaming.flinkcourse.model.commodity.SilverCommodity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SlidingWindowJoinExercise {
    public final static OutputTag<GoldCommodity> goldOutputTag = new OutputTag<>("gold-output", TypeInformation.of(GoldCommodity.class));
    public final static OutputTag<SilverCommodity> silverOutputTag = new OutputTag<>("silver-output", TypeInformation.of(SilverCommodity.class));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Void> commodityStream = env
                .addSource(new CommodityDataSource())
                .process(new CommoditySplitProcessFunction());

        DataStream<GoldCommodity> goldCommodities = commodityStream.getSideOutput(goldOutputTag)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<GoldCommodity>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((goldCommodity, l) -> goldCommodity.getTimestamp())
                        .withIdleness(Duration.ofMillis(500)));
        DataStream<SilverCommodity> silverCommodities = commodityStream.getSideOutput(silverOutputTag)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SilverCommodity>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((silverCommodity, l) -> silverCommodity.getTimestamp())
                        .withIdleness(Duration.ofMillis(500)));

        DataStream<Tuple2<String, Tuple2<Double, Double>>> goldReturns = goldCommodities
                .keyBy(goldCommodity -> goldCommodity.getExchange())
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .apply(new ReturnsWindowFunction<>());

        DataStream<Tuple2<String, Tuple2<Double, Double>>> silverReturns = silverCommodities
                .keyBy(silverCommodity -> silverCommodity.getExchange())
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .apply(new ReturnsWindowFunction<>());

        goldReturns.join(silverReturns)
                .where(stringTuple2Tuple2 -> stringTuple2Tuple2.f0)
                .equalTo(stringTuple2Tuple2 -> stringTuple2Tuple2.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .apply(new CorrelationJoinFunction())
                .filter(correlation -> correlation.f1 < 0.5)
                .print();

        env.execute("Detecting Correlated Price Movements");
    }
}
