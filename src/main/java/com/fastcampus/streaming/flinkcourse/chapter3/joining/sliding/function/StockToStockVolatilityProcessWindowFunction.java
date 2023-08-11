package com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.function;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.window.StockVolatility;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class StockToStockVolatilityProcessWindowFunction extends ProcessWindowFunction<Stock, StockVolatility, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Stock, StockVolatility, String, TimeWindow>.Context context, Iterable<Stock> iterable, Collector<StockVolatility> collector) throws Exception {
        List<Stock> sortedStocks = StreamSupport.stream(iterable.spliterator(), false)
                .sorted(Comparator.comparingLong(Stock::getTimestamp))
                .collect(Collectors.toList());

        if (!sortedStocks.isEmpty()) {
            List<Double> prices = new ArrayList<>();
            for (Stock stock : sortedStocks) {
                prices.add(stock.getPrice());
            }

            double volatility = calculateVolatility(prices);

            collector.collect(new StockVolatility(s, volatility, context.window().getEnd()));
        }
    }

    private double calculateVolatility(List<Double> prices) {
        // Calculate log returns.
        List<Double> logReturns = new ArrayList<>();
        for (int i = 0; i < prices.size() - 1; i++) {
            double denominator = prices.get(i);
            if (denominator != 0.0) {
                logReturns.add(Math.log(prices.get(i + 1) / denominator));
            }
        }

        // Calculate mean log return.
        double sum = 0.0;
        for (double logReturn : logReturns) {
            sum += logReturn;
        }
        double mean = sum / logReturns.size();

        // Calculate variance of log returns.
        double variance = 0.0;
        for (double logReturn : logReturns) {
            variance += Math.pow(logReturn - mean, 2);
        }
        variance /= (logReturns.size() - 1);

        // Ensure variance is not negative.
        variance = Math.max(0.0, variance);

        // The standard deviation of log returns is the square root of the variance.
        return Math.sqrt(variance);
    }
}
