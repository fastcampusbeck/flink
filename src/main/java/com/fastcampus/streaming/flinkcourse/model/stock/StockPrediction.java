package com.fastcampus.streaming.flinkcourse.model.stock;

public class StockPrediction {
    private String symbol;
    private double predictedPrice;
    private long timestamp;

    public StockPrediction() {
    }

    public StockPrediction(String symbol, double predictedPrice, long timestamp) {
        this.symbol = symbol;
        this.predictedPrice = predictedPrice;
        this.timestamp = timestamp;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getPredictedPrice() {
        return predictedPrice;
    }

    public void setPredictedPrice(double predictedPrice) {
        this.predictedPrice = predictedPrice;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "StockPrediction{" +
                "symbol='" + symbol + '\'' +
                ", predictedPrice=" + predictedPrice +
                ", timestamp=" + timestamp +
                '}';
    }
}
