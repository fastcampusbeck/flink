package com.fastcampus.streaming.flinkcourse.chapter3.basic.keyby.function;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithCount;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

public class StockCountMapFunction implements MapFunction<Stock, StockWithCount> {
    private final HashMap<String, Long> symbolToCount = new HashMap<>();

    @Override
    public StockWithCount map(Stock stock) throws Exception {
        String symbol = stock.getSymbol();
        long newCount = symbolToCount.getOrDefault(symbol, 0L) + 1;
        symbolToCount.put(symbol, newCount);
        return new StockWithCount(symbol, stock.getPrice(), stock.getTimestamp(), newCount);
    }
}
