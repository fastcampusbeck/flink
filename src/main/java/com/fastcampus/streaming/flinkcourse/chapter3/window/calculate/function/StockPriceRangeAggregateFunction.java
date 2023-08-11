package com.fastcampus.streaming.flinkcourse.chapter3.window.calculate.function;

import com.fastcampus.streaming.flinkcourse.model.aggregate.StockPriceRange;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.functions.AggregateFunction;

public class StockPriceRangeAggregateFunction implements AggregateFunction<Stock, StockPriceRange, StockPriceRange> {
    @Override
    public StockPriceRange createAccumulator() {
        return new StockPriceRange(Double.MAX_VALUE, Double.MIN_VALUE);
    }

    @Override
    public StockPriceRange add(Stock stock, StockPriceRange stockPriceRange) {
        double newMin = Math.min(stock.getPrice(), stockPriceRange.getMin());
        double newMax = Math.max(stock.getPrice(), stockPriceRange.getMax());

        return new StockPriceRange(newMin, newMax);
    }

    @Override
    public StockPriceRange getResult(StockPriceRange stockPriceRange) {
        return stockPriceRange;
    }

    @Override
    public StockPriceRange merge(StockPriceRange stockPriceRange, StockPriceRange acc1) {
        double mergedMin = Math.min(stockPriceRange.getMin(), acc1.getMin());
        double mergedMax = Math.max(stockPriceRange.getMax(), acc1.getMax());

        return new StockPriceRange(mergedMin, mergedMax);
    }
}
