package com.fastcampus.streaming.flinkcourse.chapter4.liststate.function;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedList;

public class MovingAverageKeyedProcessFunction extends KeyedProcessFunction<String, Stock, String> {
    private transient ListState<Double> priceState;
    private final int historySize;

    public MovingAverageKeyedProcessFunction(int historySize) {
        this.historySize = historySize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Double> descriptor = new ListStateDescriptor<>(
                "priceState", Double.class);
        priceState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(Stock stock, KeyedProcessFunction<String, Stock, String>.Context context, Collector<String> collector) throws Exception {
        Iterable<Double> currentPriceState = priceState.get();
        LinkedList<Double> prices = new LinkedList<>();

        for (Double price : currentPriceState) {
            prices.add(price);
        }

        if (prices.size() > historySize) {
            prices.removeFirst();
        }

        prices.addLast(stock.getPrice());
        priceState.update(prices);

        double sum = 0;
        for (Double price : prices) {
            sum += price;
        }
        double movingAverage = sum / prices.size();

        collector.collect("Moving average for " + stock.getSymbol() + ": " + movingAverage);
    }
}
