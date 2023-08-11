package com.fastcampus.streaming.flinkcourse.chapter3.iterate.function;

import com.fastcampus.streaming.flinkcourse.model.stock.StockWithConverged;
import org.apache.flink.api.common.functions.MapFunction;

public class CalculateEMAConvergedMapFunction implements MapFunction<StockWithConverged, StockWithConverged> {
    private double previousEMA = -1;
    private final double multiplier;
    private final double threshold;

    public CalculateEMAConvergedMapFunction(int periods, double threshold) {
        this.multiplier = 2.0 / (periods + 1.0);
        this.threshold = threshold;
    }

    @Override
    public StockWithConverged map(StockWithConverged stockWithConverged) throws Exception {
        if (previousEMA == -1) {
            previousEMA = stockWithConverged.getPrice();
            return stockWithConverged;
        }

        double newEMA = (stockWithConverged.getPrice() - previousEMA) * multiplier + previousEMA;

        if (Math.abs(newEMA - previousEMA) < threshold) {
            stockWithConverged.setConverged(true); // signal for convergence
        } else {
            previousEMA = newEMA;
            stockWithConverged.setPrice(newEMA);
        }

        return stockWithConverged;
    }
}
