package com.fastcampus.streaming.flinkcourse.chapter4.reducingstate.function;

import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

public class TradeSpikeMonitorKeyedProcessFunction  extends KeyedProcessFunction<String, StockTransaction, String> {
    private transient ReducingState<Double> maxTradeValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ReducingStateDescriptor<Double> descriptor = new ReducingStateDescriptor<>(
                "maxTradeValue", Math::max, Double.class);
        maxTradeValueState = getRuntimeContext().getReducingState(descriptor);
    }

    @Override
    public void processElement(StockTransaction stockTransaction, KeyedProcessFunction<String, StockTransaction, String>.Context context, Collector<String> collector) throws Exception {
        double currentMaxValue = Optional.ofNullable(maxTradeValueState.get()).orElse(0.0);

        double newTradeValue = stockTransaction.getPrice() * stockTransaction.getVolume();
        if (newTradeValue > currentMaxValue * 1.2) { // spike of 20%
            collector.collect("Spike detected for " + stockTransaction.getSymbol() + "! New trade value: " + newTradeValue + ", previous max value: " + currentMaxValue);
        }
        maxTradeValueState.add(newTradeValue);
    }
}
