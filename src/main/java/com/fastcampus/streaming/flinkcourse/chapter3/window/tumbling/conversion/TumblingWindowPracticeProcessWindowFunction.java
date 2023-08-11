package com.fastcampus.streaming.flinkcourse.chapter3.window.tumbling.conversion;

import com.fastcampus.streaming.flinkcourse.model.stock.StockWithCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TumblingWindowPracticeProcessWindowFunction extends ProcessWindowFunction<StockWithCount, StockWithCount, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<StockWithCount, StockWithCount, String, TimeWindow>.Context context, Iterable<StockWithCount> iterable, Collector<StockWithCount> collector) throws Exception {
        long count = 0;
        double total = 0;
        long timestamp = 0;

        for (StockWithCount stock : iterable) {
            total += stock.getPrice() * stock.getCount();
            count += stock.getCount();
            timestamp = Math.max(timestamp, stock.getTimestamp());
        }

        double avgPrice = total / count;
        collector.collect(new StockWithCount(s, avgPrice, timestamp, count));
    }
}
