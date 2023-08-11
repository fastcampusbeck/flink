package com.fastcampus.streaming.flinkcourse.model.stock;

public class StockWithCount extends Stock {
    private long count;

    public StockWithCount() {
        super();
    }

    public StockWithCount(String symbol, double price, long timestamp, long count) {
        super(symbol, price, timestamp);
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "StockWithCount{" +
                "symbol='" + getSymbol() + '\'' +
                ", price=" + getPrice() +
                ", timestamp=" + getTimestamp() +
                ", count=" + count +
                '}';
    }
}
