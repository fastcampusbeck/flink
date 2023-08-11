package com.fastcampus.streaming.flinkcourse.model.stock;

public class StockTransaction extends Stock {
    private long volume;

    public StockTransaction() {
        super();
    }

    public StockTransaction(String symbol, double price, long timestamp, long volume) {
        super(symbol, price, timestamp);
        this.volume = volume;
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
        this.volume = volume;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "symbol='" + getSymbol() + '\'' +
                ", price=" + getPrice() +
                ", timestamp=" + getTimestamp() +
                ", volume=" + volume +
                '}';
    }
}
