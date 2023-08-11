package com.fastcampus.streaming.flinkcourse.chapter3.window.session.function;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RealizedVolatilityCalculatorProcessWindowFunction extends ProcessWindowFunction<Stock, String, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Stock, String, String, TimeWindow>.Context context, Iterable<Stock> iterable, Collector<String> collector) throws Exception {
        List<Stock> sortedStocks = StreamSupport.stream(iterable.spliterator(), false)
                .sorted(Comparator.comparingLong(Stock::getTimestamp))
                .collect(Collectors.toList());

        double sum = 0;
        double squaredSum = 0;
        int count = 0;

        for (int i = 1; i < sortedStocks.size(); i++) {
            Stock currentStock = sortedStocks.get(i);
            Stock previousStock = sortedStocks.get(i - 1);

            // log_return = log(price_t / price_(t-1))
            double logReturn = Math.log(currentStock.getPrice() / previousStock.getPrice());

            sum += logReturn;
            squaredSum += logReturn * logReturn;
            count++;
        }

        if (count > 0) {
            double average = sum / count;
            double variance = (squaredSum / count) - (average * average);
            double volatility = Math.sqrt(variance);

            collector.collect("Symbol: " + s + ", Realized Volatility: " + volatility);
        }
    }
}
