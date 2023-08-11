package com.fastcampus.streaming.flinkcourse.chapter3.joining.tumbling.function;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ClosingPriceProcessWindowFunction extends ProcessWindowFunction<Stock, Stock, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Stock, Stock, String, TimeWindow>.Context context, Iterable<Stock> iterable, Collector<Stock> collector) throws Exception {
        List<Stock> sortedStocks = StreamSupport.stream(iterable.spliterator(), false)
                .sorted(Comparator.comparing(Stock::getTimestamp))
                .collect(Collectors.toList());

        if (!sortedStocks.isEmpty()) {
            collector.collect(sortedStocks.get(sortedStocks.size() - 1));
        }
    }
}
