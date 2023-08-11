package com.fastcampus.streaming.flinkcourse.chapter3.window.sliding.function;

import com.fastcampus.streaming.flinkcourse.model.aggregate.TotalTradedVolume;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.functions.MapFunction;

public class StockTransactionToTotalTradedVolumeMapFunction implements MapFunction<StockTransaction, TotalTradedVolume> {
    @Override
    public TotalTradedVolume map(StockTransaction stockTransaction) throws Exception {
        return new TotalTradedVolume(stockTransaction.getSymbol(), stockTransaction.getVolume());
    }
}