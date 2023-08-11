package com.fastcampus.streaming.flinkcourse.model.aggregate;

public class TotalTradedVolume {
    private String symbol;
    private double totalVolume;

    public TotalTradedVolume() {
    }

    public TotalTradedVolume(String symbol, double totalVolume) {
        this.symbol = symbol;
        this.totalVolume = totalVolume;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getTotalVolume() {
        return totalVolume;
    }

    public void setTotalVolume(double price) {
        this.totalVolume = price;
    }

    @Override
    public String toString() {
        return "TotalTradedVolume{" +
                "symbol='" + symbol + '\'' +
                ", totalVolume=" + totalVolume +
                '}';
    }
}
