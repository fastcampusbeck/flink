package com.fastcampus.streaming.flinkcourse.chapter3.basic.map.conversion;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithCommission;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MapExerciseProcessFunction extends ProcessFunction<Stock, StockWithCommission> {
    private static final double COMMISSION_RATE = 0.02;

    @Override
    public void processElement(Stock stock, ProcessFunction<Stock, StockWithCommission>.Context context, Collector<StockWithCommission> collector) throws Exception {
        double priceWithCommission = stock.getPrice() * (1 + COMMISSION_RATE);
        collector.collect(new StockWithCommission(
                stock.getSymbol(), priceWithCommission, stock.getTimestamp(), COMMISSION_RATE));
    }
}
