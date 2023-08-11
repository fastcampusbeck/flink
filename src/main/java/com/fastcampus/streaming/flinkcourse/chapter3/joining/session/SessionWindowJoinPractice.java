package com.fastcampus.streaming.flinkcourse.chapter3.joining.session;

import com.fastcampus.streaming.flinkcourse.chapter3.joining.session.source.EconomicIndicatorSourceFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.joining.session.function.IndicatorBasedTimeGapExtractor;
import com.fastcampus.streaming.flinkcourse.chapter3.joining.session.source.TradingActivitySource;
import com.fastcampus.streaming.flinkcourse.model.trade.EconomicIndicator;
import com.fastcampus.streaming.flinkcourse.model.trade.Trade;
import com.fastcampus.streaming.flinkcourse.model.trade.TradeWithIndicator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;

import java.time.Duration;

public class SessionWindowJoinPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Trade> nyseTradeStream = env.addSource(new TradingActivitySource("NYSE"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Trade>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                                .withIdleness(Duration.ofMillis(500)));

        DataStream<Trade> lseTradeStream = env.addSource(new TradingActivitySource("LSE"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Trade>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                                .withIdleness(Duration.ofMillis(500)));

        DataStream<TradeWithIndicator> tradingActivityStream = nyseTradeStream.union(lseTradeStream)
                .map(trade -> new TradeWithIndicator(trade.getExchange(), trade.getSecurityId(), trade.getTimestamp(),
                        trade.getTradePrice(), trade.getTradeVolume(), "GDP"));

        DataStream<EconomicIndicator> economicIndicatorStream = env.addSource(new EconomicIndicatorSourceFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EconomicIndicator>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                                .withIdleness(Duration.ofMillis(500)));

        tradingActivityStream.join(economicIndicatorStream)
                .where(TradeWithIndicator::getIndicator)
                .equalTo(EconomicIndicator::getName)
                .window(EventTimeSessionWindows.withDynamicGap(new IndicatorBasedTimeGapExtractor()))
                .apply((first, second) -> "Exchange: " + first.getExchange() + ", Trade Volume: " + first.getTradeVolume() +
                        ", Indicator: GDP" + ", Economic Indicator Value: " + second.getValue())
                .print();

        env.execute("Trading Activity Indicator Join");
    }
}
