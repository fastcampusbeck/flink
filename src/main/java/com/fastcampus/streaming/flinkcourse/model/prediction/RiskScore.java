package com.fastcampus.streaming.flinkcourse.model.prediction;

public class RiskScore {
    private String symbol;
    private long timestamp;
    private double riskScore;

    public RiskScore() {
    }

    public RiskScore(String symbol, long timestamp, double riskScore) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.riskScore = riskScore;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getRiskScore() {
        return riskScore;
    }

    public void setRiskScore(double riskScore) {
        this.riskScore = riskScore;
    }

    @Override
    public String toString() {
        return "RiskScore{" +
                "symbol='" + symbol + '\'' +
                ", timestamp=" + timestamp +
                ", riskScore=" + riskScore +
                '}';
    }

}
