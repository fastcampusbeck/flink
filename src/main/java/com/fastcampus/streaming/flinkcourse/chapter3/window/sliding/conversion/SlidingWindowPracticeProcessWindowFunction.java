package com.fastcampus.streaming.flinkcourse.chapter3.window.sliding.conversion;

import com.fastcampus.streaming.flinkcourse.model.aggregate.TotalTradedVolume;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SlidingWindowPracticeProcessWindowFunction extends ProcessWindowFunction<StockTransaction, TotalTradedVolume, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<StockTransaction, TotalTradedVolume, String, TimeWindow>.Context context, Iterable<StockTransaction> iterable, Collector<TotalTradedVolume> collector) throws Exception {
        double totalVolume = 0;

        for (StockTransaction stockTransaction : iterable) {
            totalVolume += stockTransaction.getVolume();
        }

        collector.collect(new TotalTradedVolume(s, totalVolume));
    }
}
