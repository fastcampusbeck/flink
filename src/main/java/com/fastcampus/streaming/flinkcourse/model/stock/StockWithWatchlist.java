package com.fastcampus.streaming.flinkcourse.model.stock;

import java.util.Arrays;
import java.util.List;

public class StockWithWatchlist extends Stock {
    private final List<String> watchlist = Arrays.asList("FB", "AMZN", "AAPL", "NFLX", "GOOG");

    public StockWithWatchlist(String symbol, double price, long timestamp) {
        super(symbol, price, timestamp);
    }

    public List<String> getWatchlist() {
        return watchlist;
    }
}
