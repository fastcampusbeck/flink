package com.fastcampus.streaming.flinkcourse.model.asset;

public class AssetTrade {
    private String asset;
    private boolean isBuy;
    private double price;
    private int amount;

    public AssetTrade() {}

    public AssetTrade(String asset, boolean isBuy, double price, int amount) {
        this.asset = asset;
        this.isBuy = isBuy;
        this.price = price;
        this.amount = amount;
    }

    public String getAsset() {
        return asset;
    }

    public void setAsset(String asset) {
        this.asset = asset;
    }

    public boolean getIsBuy() {
        return isBuy;
    }

    public void setIsBuy(boolean buy) {
        isBuy = buy;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "AssetTrade{" +
                "asset='" + asset + '\'' +
                ", isBuy=" + isBuy +
                ", price=" + price +
                ", amount=" + amount +
                '}';
    }
}
