package com.fastcampus.streaming.flinkcourse.model.asset;

public class AssetTradeWithRisk extends AssetTradeWithCustomer {
    private double riskWeight;

    public AssetTradeWithRisk() {
    }

    public AssetTradeWithRisk(String asset, boolean isBuy, double price, int amount, String customerId, double riskWeight) {
        super(asset, isBuy, price, amount, customerId);
        this.riskWeight = riskWeight;
    }

    public double getRiskWeight() {
        return riskWeight;
    }

    public void setRiskWeight(double riskWeight) {
        this.riskWeight = riskWeight;
    }

    @Override
    public String toString() {
        return "AssetTradeWithRisk{" +
                "asset='" + getAsset() + '\'' +
                ", isBuy=" + getIsBuy() +
                ", price=" + getPrice() +
                ", amount=" + getAmount() +
                ", customerId='" + getCustomerId() + '\'' +
                ", riskWeight=" + riskWeight +
                '}';
    }
}
