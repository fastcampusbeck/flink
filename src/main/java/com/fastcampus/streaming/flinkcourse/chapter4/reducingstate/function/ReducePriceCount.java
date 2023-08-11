package com.fastcampus.streaming.flinkcourse.chapter4.reducingstate.function;

import com.fastcampus.streaming.flinkcourse.model.aggregate.PriceCount;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ReducePriceCount implements ReduceFunction<PriceCount> {
    @Override
    public PriceCount reduce(PriceCount priceCount, PriceCount t1) throws Exception {
        return new PriceCount(priceCount.getPrice() + t1.getPrice(), priceCount.getCount() + t1.getCount());
    }
}
