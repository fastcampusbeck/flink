package com.fastcampus.streaming.flinkcourse.chapter3.window.global.evictor;

import com.fastcampus.streaming.flinkcourse.model.portfolio.Portfolio;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PortfolioValidTimestampEvictor implements Evictor<Portfolio, GlobalWindow> {
    @Override
    public void evictBefore(Iterable<TimestampedValue<Portfolio>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {
        List<TimestampedValue<Portfolio>> portfolioList = StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
        portfolioList.removeIf(portfolioTimestampedValue -> !portfolioTimestampedValue.getValue().isValid(evictorContext.getCurrentWatermark()));
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<Portfolio>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {
    }
}
