package com.fastcampus.streaming.flinkcourse.chapter3.basic.reduce.function;

import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.functions.ReduceFunction;

public class MaxVolumeReduceFunction implements ReduceFunction<StockTransaction> {
    @Override
    public StockTransaction reduce(StockTransaction stockTransaction, StockTransaction t1) throws Exception {
        if (stockTransaction.getVolume() > t1.getVolume()) {
            return stockTransaction;
        } else {
            return t1;
        }
    }
}