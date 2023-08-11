package com.fastcampus.streaming.flinkcourse.chapter3.basic.flatmap.function;

import com.fastcampus.streaming.flinkcourse.model.stock.SectorStock;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class SectorStockFlatMapFunction implements FlatMapFunction<String, SectorStock> {
    @Override
    public void flatMap(String s, Collector<SectorStock> collector) throws Exception {
        String[] parts = s.split("\\|");
        String sector = parts[0];
        String[] stocks = parts[1].split(",");
        for (String stock : stocks) {
            String[] stockData = stock.split(":");
            String symbol = stockData[0];
            double price = Double.parseDouble(stockData[1]);
            long timestamp = Long.parseLong(stockData[2]);
            collector.collect(new SectorStock(symbol, price, timestamp, sector));
        }
    }
}
