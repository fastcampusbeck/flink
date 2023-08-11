package com.fastcampus.streaming.flinkcourse.model.asset;

public class PriceUpdate {
    private String asset;
    private double price;

    public PriceUpdate() {
    }

    public PriceUpdate(String asset, double price) {
        this.asset = asset;
        this.price = price;
    }

    public String getAsset() {
        return asset;
    }

    public void setAsset(String asset) {
        this.asset = asset;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "PriceUpdate{" +
                "asset='" + asset + '\'' +
                ", price=" + price +
                '}';
    }
}
