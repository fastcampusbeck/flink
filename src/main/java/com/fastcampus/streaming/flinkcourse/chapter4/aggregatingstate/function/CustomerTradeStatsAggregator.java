package com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.function;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithCustomer;
import com.fastcampus.streaming.flinkcourse.model.trade.CustomerTradeStats;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CustomerTradeStatsAggregator implements AggregateFunction<AssetTradeWithCustomer, CustomerTradeStats, Double> {
    @Override
    public CustomerTradeStats createAccumulator() {
        return new CustomerTradeStats();
    }

    @Override
    public CustomerTradeStats add(AssetTradeWithCustomer assetTradeWithCustomer, CustomerTradeStats accumulator) {
        accumulator.setNetProfit(accumulator.getNetProfit() + assetTradeWithCustomer.getPrice());
        accumulator.setTradeCount(accumulator.getTradeCount() + 1);
        return accumulator;
    }

    @Override
    public Double getResult(CustomerTradeStats accumulator) {
        return accumulator.getTradeCount() != 0 ? accumulator.getNetProfit() / accumulator.getTradeCount() : 0;
    }

    @Override
    public CustomerTradeStats merge(CustomerTradeStats a, CustomerTradeStats b) {
        a.setNetProfit(a.getNetProfit() + b.getNetProfit());
        a.setTradeCount(a.getTradeCount() + b.getTradeCount());
        return a;
    }
}