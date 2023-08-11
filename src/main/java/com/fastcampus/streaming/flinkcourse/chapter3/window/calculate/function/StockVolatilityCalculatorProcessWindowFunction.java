package com.fastcampus.streaming.flinkcourse.chapter3.window.calculate.function;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StockVolatilityCalculatorProcessWindowFunction  extends ProcessWindowFunction<Stock, Double, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Stock, Double, String, TimeWindow>.Context context, Iterable<Stock> iterable, Collector<Double> collector) throws Exception {
        double sum = 0;
        double sumOfSquares = 0;
        int count = 0;

        for (Stock stock : iterable) {
            double price = stock.getPrice();
            sum += price;
            sumOfSquares += price * price;
            count++;
        }

        double mean = sum / count;
        double variance = sumOfSquares / count - mean * mean;
        double stdDev = Math.sqrt(variance);

        collector.collect(stdDev);
    }
}
