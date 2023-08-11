package com.fastcampus.streaming.flinkcourse.chapter3.joining.tumbling.function;

import com.fastcampus.streaming.flinkcourse.model.aggregate.TotalTradedVolume;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.functions.AggregateFunction;

public class StockTransactionsToTotalTradedVolumeAggregateFunction implements AggregateFunction<StockTransaction, TotalTradedVolume, TotalTradedVolume> {
    @Override
    public TotalTradedVolume createAccumulator() {
        return new TotalTradedVolume("", 0.0);
    }

    @Override
    public TotalTradedVolume add(StockTransaction stockTransaction, TotalTradedVolume totalTradedVolume) {
        if (totalTradedVolume.getSymbol().isEmpty()) {
            totalTradedVolume.setSymbol(stockTransaction.getSymbol());
        }

        totalTradedVolume.setTotalVolume(totalTradedVolume.getTotalVolume() + stockTransaction.getVolume());
        return totalTradedVolume;
    }

    @Override
    public TotalTradedVolume getResult(TotalTradedVolume totalTradedVolume) {
        return totalTradedVolume;
    }

    @Override
    public TotalTradedVolume merge(TotalTradedVolume totalTradedVolume, TotalTradedVolume acc1) {
        return new TotalTradedVolume(totalTradedVolume.getSymbol(), totalTradedVolume.getTotalVolume() + acc1.getTotalVolume());
    }
}
