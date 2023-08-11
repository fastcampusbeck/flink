package com.fastcampus.streaming.flinkcourse.chapter3.window.sliding.function;

import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.functions.ReduceFunction;

public class StockVolumeReduceFunction implements ReduceFunction<StockTransaction> {
    @Override
    public StockTransaction reduce(StockTransaction stockTransaction, StockTransaction t1) throws Exception {
        return new StockTransaction(stockTransaction.getSymbol(),
                stockTransaction.getPrice(), // Temp value
                stockTransaction.getTimestamp(), // Temp value
                stockTransaction.getVolume() + t1.getVolume());
    }
}