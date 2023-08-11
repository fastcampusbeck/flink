package com.fastcampus.streaming.flinkcourse.model.stock;

public class StockWithCommission extends Stock {
    private double commission;

    public StockWithCommission() {
        super();
    }

    public StockWithCommission(String symbol, double price, long timestamp, double commission) {
        super(symbol, price, timestamp);
        this.commission = commission;
    }

    public double getCommission() {
        return commission;
    }

    public void setCommission(double commission) {
        this.commission = commission;
    }

    @Override
    public String toString() {
        return "StockPriceWithCommission{" +
                "symbol='" + getSymbol() + '\'' +
                ", price=" + getPrice() +
                ", timestamp=" + getTimestamp() +
                ", commission=" + commission +
                '}';
    }
}
