package com.fastcampus.streaming.flinkcourse.chapter4.reducingstate.function;

import com.fastcampus.streaming.flinkcourse.model.aggregate.PriceCount;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

public class AveragePriceMonitorKeyedProcessFunction extends KeyedProcessFunction<String, Stock, String> {
    private transient ReducingState<PriceCount> priceCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ReducingStateDescriptor<PriceCount> descriptor = new ReducingStateDescriptor<>(
                "priceCount", new ReducePriceCount(), PriceCount.class);
        priceCountState = getRuntimeContext().getReducingState(descriptor);    }

    @Override
    public void processElement(Stock stock, KeyedProcessFunction<String, Stock, String>.Context context, Collector<String> collector) throws Exception {
        PriceCount currentPriceCount = Optional.ofNullable(priceCountState.get()).orElse(new PriceCount(0.0, 0L));

        double currentAveragePrice = currentPriceCount.getPrice() / currentPriceCount.getCount();
        if (Math.abs(stock.getPrice() - currentAveragePrice) > currentAveragePrice * 0.05) { // deviation of 5%
            collector.collect("Significant price change for " + stock.getSymbol() + "! New price: " + stock.getPrice() + ", average price: " + currentAveragePrice);
        }
        priceCountState.add(new PriceCount(stock.getPrice(), 1));
    }
}
