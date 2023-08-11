package com.fastcampus.streaming.flinkcourse.chapter3.window.global.function;

import com.fastcampus.streaming.flinkcourse.model.aggregate.TotalTradedVolume;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class TotalTransactionAmountCalculatorProcessWindowFunction extends ProcessWindowFunction<StockTransaction, TotalTradedVolume, String, GlobalWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<StockTransaction, TotalTradedVolume, String, GlobalWindow>.Context context, Iterable<StockTransaction> iterable, Collector<TotalTradedVolume> collector) throws Exception {
        double totalTransactionAmount = 0;
        for (StockTransaction stockTransaction : iterable) {
            totalTransactionAmount += stockTransaction.getPrice() * stockTransaction.getVolume();
        }
        collector.collect(new TotalTradedVolume(s, totalTransactionAmount));
    }
}
