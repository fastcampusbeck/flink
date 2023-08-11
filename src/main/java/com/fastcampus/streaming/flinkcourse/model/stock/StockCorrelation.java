package com.fastcampus.streaming.flinkcourse.model.stock;

public class StockCorrelation {
    private String stockA;
    private String stockB;
    private double correlation;

    public StockCorrelation() {
    }

    public StockCorrelation(String stockA, String stockB, double correlation) {
        this.stockA = stockA;
        this.stockB = stockB;
        this.correlation = correlation;
    }

    public String getStockA() {
        return stockA;
    }

    public void setStockA(String stockA) {
        this.stockA = stockA;
    }

    public String getStockB() {
        return stockB;
    }

    public void setStockB(String stockB) {
        this.stockB = stockB;
    }

    public double getCorrelation() {
        return correlation;
    }

    public void setCorrelation(double correlation) {
        this.correlation = correlation;
    }

    @Override
    public String toString() {
        return "StockCorrelation{" +
                "stockA='" + stockA + '\'' +
                ", stockB='" + stockB + '\'' +
                ", correlation=" + correlation +
                '}';
    }
}
