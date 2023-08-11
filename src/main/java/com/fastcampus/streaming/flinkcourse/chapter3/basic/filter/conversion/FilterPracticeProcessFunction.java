package com.fastcampus.streaming.flinkcourse.chapter3.basic.filter.conversion;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithWatchlist;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class FilterPracticeProcessFunction extends ProcessFunction<Stock, StockWithWatchlist> {
    @Override
    public void processElement(Stock stock, ProcessFunction<Stock, StockWithWatchlist>.Context context, Collector<StockWithWatchlist> collector) throws Exception {
        StockWithWatchlist stockWithWatchlist = new StockWithWatchlist(stock.getSymbol(), stock.getPrice(), stock.getTimestamp());
        if (stockWithWatchlist.getWatchlist().contains(stockWithWatchlist.getSymbol())) {
            collector.collect(stockWithWatchlist);
        }
    }
}
