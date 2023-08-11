package com.fastcampus.streaming.flinkcourse.model.asset;

public class RiskUpdate {
    private String asset;
    private double riskWeight;

    public RiskUpdate() {
    }

    public RiskUpdate(String asset, double riskWeight) {
        this.asset = asset;
        this.riskWeight = riskWeight;
    }

    public String getAsset() {
        return asset;
    }

    public void setAsset(String asset) {
        this.asset = asset;
    }

    public double getRiskWeight() {
        return riskWeight;
    }

    public void setRiskWeight(double riskWeight) {
        this.riskWeight = riskWeight;
    }

    @Override
    public String toString() {
        return "RiskUpdate{" +
                "asset='" + asset + '\'' +
                ", riskWeight=" + riskWeight +
                '}';
    }
}
