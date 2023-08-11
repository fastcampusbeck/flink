package com.fastcampus.streaming.flinkcourse.model.aggregate;

public class AverageStockPrice {
    private String symbol;
    private double averagePrice;

    public AverageStockPrice() {
    }

    public AverageStockPrice(String symbol, double averagePrice) {
        this.symbol = symbol;
        this.averagePrice = averagePrice;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getAveragePrice() {
        return averagePrice;
    }

    public void setAveragePrice(double averagePrice) {
        this.averagePrice = averagePrice;
    }

    @Override
    public String toString() {
        return "AverageStockPrice{" +
                "symbol='" + symbol + '\'' +
                ", averagePrice=" + averagePrice +
                '}';
    }
}
