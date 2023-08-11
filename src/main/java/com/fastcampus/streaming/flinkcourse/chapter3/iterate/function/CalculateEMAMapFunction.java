package com.fastcampus.streaming.flinkcourse.chapter3.iterate.function;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.functions.MapFunction;

public class CalculateEMAMapFunction implements MapFunction<Stock, Stock> {
    private double previousEMA = -1;
    private final double multiplier;

    public CalculateEMAMapFunction(int periods) {
        this.multiplier = 2.0 / (periods + 1.0);
    }

    @Override
    public Stock map(Stock stock) throws Exception {
        if (previousEMA == -1) {
            previousEMA = stock.getPrice();
        } else {
            previousEMA = (stock.getPrice() - previousEMA) * multiplier + previousEMA;
        }
        stock.setPrice(previousEMA);
        return stock;
    }
}