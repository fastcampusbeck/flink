package com.fastcampus.streaming.flinkcourse.chapter3.window.sliding.function;

import com.fastcampus.streaming.flinkcourse.model.aggregate.AverageStockPrice;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class AverageStockPriceAggregateFunction implements AggregateFunction<Stock, Tuple3<String, Double, Long>, AverageStockPrice> {
    @Override
    public Tuple3<String, Double, Long> createAccumulator() {
        return Tuple3.of("", 0.0, 0L);
    }

    @Override
    public Tuple3<String, Double, Long> add(Stock stock, Tuple3<String, Double, Long> stringDoubleLongTuple3) {
        return Tuple3.of(stock.getSymbol(), stringDoubleLongTuple3.f1 + stock.getPrice(), stringDoubleLongTuple3.f2 + 1);
    }

    @Override
    public AverageStockPrice getResult(Tuple3<String, Double, Long> stringDoubleLongTuple3) {
        double avgPrice = stringDoubleLongTuple3.f1 / stringDoubleLongTuple3.f2;
        return new AverageStockPrice(stringDoubleLongTuple3.f0, avgPrice);
    }

    @Override
    public Tuple3<String, Double, Long> merge(Tuple3<String, Double, Long> stringDoubleLongTuple3, Tuple3<String, Double, Long> acc1) {
        return Tuple3.of(stringDoubleLongTuple3.f0, stringDoubleLongTuple3.f1 + acc1.f1, stringDoubleLongTuple3.f2 + acc1.f2);
    }
}
