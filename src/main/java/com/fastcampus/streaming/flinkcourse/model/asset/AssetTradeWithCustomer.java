package com.fastcampus.streaming.flinkcourse.model.asset;

public class AssetTradeWithCustomer extends AssetTrade {
    private String customerId;

    public AssetTradeWithCustomer() {
    }

    public AssetTradeWithCustomer(String asset, boolean isBuy, double price, int amount, String customerId) {
        super(asset, isBuy, price, amount);
        this.customerId = customerId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    @Override
    public String toString() {
        return "AssetTradeWithCustomer{" +
                "AssetTrade{" +
                "asset='" + getAsset() + '\'' +
                ", isBuy=" + getIsBuy() +
                ", price=" + getPrice() +
                ", amount=" + getAmount() +
                ", customerId='" + customerId + '\'' +
                '}';
    }
}