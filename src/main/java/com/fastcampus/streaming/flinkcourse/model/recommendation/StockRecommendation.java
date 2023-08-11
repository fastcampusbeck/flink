package com.fastcampus.streaming.flinkcourse.model.recommendation;

public class StockRecommendation {
    private String stockId;
    private String recommendation; // BUY, SELL, or HOLD
    private double price;

    public StockRecommendation() {
    }

    public StockRecommendation(String stockId, String recommendation, double price) {
        this.stockId = stockId;
        this.recommendation = recommendation;
        this.price = price;
    }

    public String getStockId() {
        return stockId;
    }

    public void setStockId(String stockId) {
        this.stockId = stockId;
    }

    public String getRecommendation() {
        return recommendation;
    }

    public void setRecommendation(String recommendation) {
        this.recommendation = recommendation;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "StockRecommendation{" +
                "stockId='" + stockId + '\'' +
                ", recommendation='" + recommendation + '\'' +
                ", price=" + price +
                '}';
    }
}
