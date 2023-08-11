package com.fastcampus.streaming.flinkcourse.model.window;

public class StockVolatility {
    private String symbol;
    private double volatility;
    private long windowEnd;

    public StockVolatility() {
    }

    public StockVolatility(String symbol, double volatility, long windowEnd) {
        this.symbol = symbol;
        this.volatility = volatility;
        this.windowEnd = windowEnd;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getVolatility() {
        return volatility;
    }

    public void setVolatility(double volatility) {
        this.volatility = volatility;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "StockVolatility{" +
                "symbol='" + symbol + '\'' +
                ", volatility=" + volatility +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
