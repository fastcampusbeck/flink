package com.fastcampus.streaming.flinkcourse.model.portfolio;

public class VaR {
    private String portfolioId;
    private double value;

    public VaR() {
    }

    public VaR(String portfolioId, double value) {
        this.portfolioId = portfolioId;
        this.value = value;
    }

    public String getPortfolioId() {
        return portfolioId;
    }

    public void setPortfolioId(String portfolioId) {
        this.portfolioId = portfolioId;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "VaR{" +
                "portfolioId='" + portfolioId + '\'' +
                ", value=" + value +
                '}';
    }
}
