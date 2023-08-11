package com.fastcampus.streaming.flinkcourse.chapter3.joining.session.function;

import com.fastcampus.streaming.flinkcourse.model.trade.EconomicIndicator;
import com.fastcampus.streaming.flinkcourse.model.trade.Trade;
import com.fastcampus.streaming.flinkcourse.model.trade.TradeWithIndicator;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;

import java.util.concurrent.TimeUnit;

public class IndicatorBasedTimeGapExtractor implements SessionWindowTimeGapExtractor<CoGroupedStreams.TaggedUnion<TradeWithIndicator, EconomicIndicator>> {
    @Override
    public long extract(CoGroupedStreams.TaggedUnion<TradeWithIndicator, EconomicIndicator> tradingActivityEconomicIndicatorTaggedUnion) {
        if (tradingActivityEconomicIndicatorTaggedUnion.isTwo()) {
            EconomicIndicator economicIndicator = tradingActivityEconomicIndicatorTaggedUnion.getTwo();
            switch (economicIndicator.getName()) {
                case "GDP":
                    return TimeUnit.MILLISECONDS.toMillis(500);
                case "CPI":
                    return TimeUnit.MILLISECONDS.toMillis(1000);
                case "Unemployment Rate":
                    return TimeUnit.MILLISECONDS.toMillis(1500);
                default:
                    return TimeUnit.MILLISECONDS.toMillis(100);
            }
        } else if (tradingActivityEconomicIndicatorTaggedUnion.isOne()) {
            return TimeUnit.MILLISECONDS.toMillis(1);
        } else {
            // This should never happen, but just in case
            System.err.println("Error: tradingActivityEconomicIndicatorTaggedUnion is neither a Trade nor an EconomicIndicator");
            return TimeUnit.MILLISECONDS.toMillis(1);
        }
    }
}