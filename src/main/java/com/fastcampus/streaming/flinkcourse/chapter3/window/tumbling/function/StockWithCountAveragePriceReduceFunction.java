package com.fastcampus.streaming.flinkcourse.chapter3.window.tumbling.function;

import com.fastcampus.streaming.flinkcourse.model.stock.StockWithCount;
import org.apache.flink.api.common.functions.ReduceFunction;

public class StockWithCountAveragePriceReduceFunction implements ReduceFunction<StockWithCount> {
    @Override
    public StockWithCount reduce(StockWithCount stockWithCount, StockWithCount t1) throws Exception {
        String symbol = stockWithCount.getSymbol();
        long timestamp = t1.getTimestamp();

        double total = stockWithCount.getPrice() * stockWithCount.getCount() + t1.getPrice() * t1.getCount();
        long count = stockWithCount.getCount() + t1.getCount();
        double avgPrice = total / count;

        return new StockWithCount(symbol, avgPrice, timestamp, count);
    }
}
